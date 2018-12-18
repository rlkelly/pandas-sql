import numpy as np
import pandas as pd
from pandas.core.frame import DataFrame
from sqlalchemy import Column, Integer, String, Float
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.sql import select
from sqlalchemy.dialects import sqlite
from sqlalchemy.dialects import postgresql


def convert_types(type, primary_key=False, maxlen=None, unique=False):
    if np.issubdtype(type, np.integer):
        return Column(Integer(), primary_key=primary_key)
    if np.issubdtype(type, np.integer):
        return Column(Float(), primary_key=primary_key)
    if np.issubdtype(type, np.object_):
        return Column(String(length=maxlen), unique=unique)


class SqlDataFrame(object):
    def __init__(self, pandas_dataframe, table_name, dialect=postgresql):
        assert isinstance(pandas_dataframe, DataFrame)
        self.df = pandas_dataframe
        self.queried_df = pandas_dataframe
        self.dialect = dialect
        self.statement = None
        self.table_name = table_name
        self.init_table()

    def init_table(self):
        class Table(declarative_base()):
            __tablename__ = self.table_name
            id = Column(Integer(), primary_key=True)

        cols = self.df.columns.values
        schema = dict(((col, self.queried_df[col].dtype.type) for col in cols))
        for key in schema.keys():
            value = schema[key]
            setattr(Table, key, convert_types(value))
        self.Table = Table

    def select(self, selection):
        if type(selection) is list:
            self.statement = select([getattr(self, col) if type(col) == str else col for col in selection])
        else:
            self.statement = select([getattr(self, col) if type(col) == str else col for col in [selection]])
        return self

    def where(self, cond):
        return Where(self, cond)

    def and_where(self, cond):
        return self.where(cond)

    def reset(self):
        self.queried_df = self.df
        return self

    def extract_query(self):
        query = self.statement.compile(dialect=postgresql.dialect())
        query_str = str(query)
        for param in query.params:
            query_str = query_str.replace(f'%({param})s', str(query.params[param]))
        return query_str

    def outerjoin(self, other_table, left, right):
        self.statement = (self.statement
            .outerjoin(other_table.Table, left == right)
        )
        self.queried_df = pd.merge(
            self.queried_df,
            other_table.queried_df,
            left_on=left.name,
            right_on=right.name,
            how='outer',
        )
        return self

    def collect(self):
        self.df = self.queried_df
        return self

    def innerjoin(self, other_table, left, right):
        self.statement = (self.statement
            .outerjoin(other_table.Table, left == right)
        )
        self.queried_df = pd.merge(
            self.queried_df,
            other_table.queried_df,
            left_on=left.name,
            right_on=right.name,
            how='inner',
        )
        return self

    def __getattr__(self, attr):
        if type(attr) is str:
            if attr not in dir(self):
                return getattr(self.Table, attr)
        return getattr(self, attr)


class Where(SqlDataFrame):
    def __init__(self, parent, cond):
        self.cond = cond
        self.parent = parent
        for attr in dir(parent):
            if not attr.startswith('_'):
                setattr(self, attr, getattr(parent, attr))

    def eq(self, eq):
        if type(self.cond) is str:
            self.parent.queried_df = self.queried_df[self.queried_df[self.cond] == eq]
            self.cond = getattr(self.Table, self.cond)
        if type(eq) is str:
            eq = '"' + str(eq) + '"'
        self.parent.statement = self.statement.where(
            self.cond == eq)
        return self.parent

    def lt(self, eq):
        if type(self.cond) is str:
            self.parent.queried_df = self.df[self.df[self.cond] < eq]
            self.cond = getattr(self.Table, self.cond)
        if type(eq) is str:
            eq = '"' + str(eq) + '"'
        self.parent.statement = self.statement.where(
            self.cond < eq)
        return self.parent

    def gt(self, eq):
        if type(self.cond) is str:
            self.parent.queried_df = self.df[self.df[self.cond] > eq]
            self.cond = getattr(self.Table, self.cond)
        if type(eq) is str:
            eq = '"' + str(eq) + '"'
        self.parent.statement = self.statement.where(
            self.cond > eq)
        return self.parent

# examples
if __name__ == '__main__':
    first_dataframe = SqlDataFrame(
        pd.DataFrame({'number': range(5), 'owner': ['al', 'beau', 'chris', 'dan', 'ed']}),
        table_name='homes',
    )
    second_dataframe = SqlDataFrame(
        pd.DataFrame({'house_number': range(5), 'pet': ['cat', 'dog', 'bird', None, None]}),
        table_name='pets',
    )

    print(
        first_dataframe
            .select([first_dataframe.number, first_dataframe.owner])
            .where(first_dataframe.number).eq(1)
            .and_where('owner').eq('al')
            .extract_query(),
    )
    print('\n\n')
    first_dataframe.reset()
    print(
        first_dataframe
            .select([first_dataframe.number, second_dataframe.pet])
            .where('owner').eq('al')
            .outerjoin(
                second_dataframe,
                first_dataframe.number,
                second_dataframe.house_number,
            )
            .extract_query()
    )
    print('\n\n')
    first_dataframe.reset()
    print(
        first_dataframe
            .select([first_dataframe.number, second_dataframe.pet])
            .where('owner').eq('al')
            .innerjoin(
                second_dataframe,
                first_dataframe.number,
                second_dataframe.house_number,
            )
            .collect()
            .extract_query()
    )
