#!/usr/bin/env python3

import numpy as np
import pandas as pd
from pandas.core.frame import DataFrame
from sqlalchemy import Column, Integer
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.sql import select
from sqlalchemy.dialects import sqlite
from sqlalchemy.dialects import postgresql

from sql_pandas.utils import convert_types


class SqlDataFrame(object):
    def __init__(self, data, table_name, dialect=postgresql):
        if type(data) is dict:
            self.df = pd.DataFrame(data)
        elif isinstance(data, DataFrame):
            self.df = data
        else:
            raise Exception('Invalid Format')
        self.queried_df = self.df
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
