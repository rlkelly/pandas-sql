#!/usr/bin/env python3

# TODO: drop_duplicates
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

import numpy as np
import pandas as pd
from pandas.core.frame import DataFrame
import sqlalchemy_bigquery.base as bq
from sqlalchemy import Column, Integer
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.sql import select
from sqlalchemy.dialects import sqlite, postgresql

from sql_pandas.utils import convert_types


class SqlDataFrame(object):
    def __init__(self, data, table_name, dialect=postgresql.dialect()):
        if type(data) is dict:
            self.df = pd.DataFrame(data)
        elif isinstance(data, DataFrame):
            self.df = data
        else:
            raise Exception('Invalid Format')

        self.queried_df = self.df
        self.dialect = dialect
        self.table_name = table_name
        self.columns = None
        self.query = None
        self.init_table()
        engine = create_engine('sqlite://', echo=True)
        self.Session = sessionmaker(bind=engine)
        self.session = self.Session()
        self.iterations = []

    def init_table(self):
        class Table(declarative_base()):
            __tablename__ = self.table_name
            id = Column(Integer(), primary_key=True)

        cols = self.df.columns.values
        self.cols = cols
        schema = dict(((col, self.queried_df[col].dtype.type) for col in cols))
        for key in schema.keys():
            value = schema[key]
            setattr(Table, key, convert_types(value))
        self.Table = Table

    def select(self, *selection):
        try:
            models = [s.class_ for s in selection]
            self.columns = selection
        except:
            models = selection
        self.query = self.session.query(*models)
        return self

    def where(self, cond):
        return Where(self, cond)

    def and_where(self, cond):
        return self.where(cond)

    def reset(self):
        self.iterations.append(self.extract_query())
        self.queried_df = self.df
        self.session = self.Session()
        return self

    def collect(self):
        if self.columns:
            self.query = self.query.with_entities(*self.columns)
            self.queried_df = self.queried_df[[col.name for col in self.columns]]
        self.df = self.queried_df
        return self

    def _merge(self, other_table, left, right, how='outer'):
        self.queried_df = pd.merge(
            self.queried_df,
            other_table.queried_df,
            left_on=left.name,
            right_on=right.name,
            how='inner',
        )

    def merge(self, other_table, left, right, how='outer'):
        if how == 'outer':
            self.query = (self.query
                .outerjoin(other_table.Table, left == right)
            )
        elif how == 'inner':
            self.query = (self.query
                .innerjoin(other_table.Table, left == right)
            )
        else:
            raise NotImplemented('Working on Full Outer Still')
        self._merge(other_table, left, right, how=how)
        return self

    def outerjoin(self, other_table, left, right):
        self.query = (self.query
            .outerjoin(other_table.Table, left == right)
        )
        self._merge(other_table, left, right)
        return self

    def innerjoin(self, other_table, left, right):
        self.query = (self.query
            .join(other_table.Table, left == right)
        )
        self._merge(other_table, left, right, how='inner')
        return self

    def limit(self, num):
        self.query = self.query.limit(num)
        return self

    def dedupe(self):
        self.query = self.query.distinct()
        self.queried_df = self.queried_df.drop_duplicates()
        return self

    def __getattr__(self, attr):
        if type(attr) is str:
            if attr not in dir(self):
                return getattr(self.Table, attr)
        return getattr(self, attr)

    def extract_query(self):
        if self.columns:
            self.query = self.query.with_entities(*self.columns)
            self.queried_df = self.queried_df[[col.name for col in self.columns]]
        else:
            self.query = self.query.with_entities(*[getattr(self.Table, name) for name in self.cols])
        query = self.query.statement.compile(dialect=self.dialect)
        query_str = str(query)
        for param in query.params:
            query_str = query_str.replace(f'%({param})s', str(query.params[param]))
        return query_str + '\n\n'

    def __getitem__(self, item):
        if type(item) is list:
            return self.select(*item)
        return self.select(item)


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
            eq = f'"{eq}"'
        self.parent.query = self.parent.query.filter(
            self.cond == eq)
        return self.parent

    def ne(self, eq):
        if type(self.cond) is str:
            self.parent.queried_df = self.queried_df[self.queried_df[self.cond] != eq]
            self.cond = getattr(self.Table, self.cond)
        if type(eq) is str:
            eq = '"' + str(eq) + '"'
        self.parent.query = self.parent.query.filter(
            self.cond != eq)
        return self.parent

    def lt(self, eq):
        if type(self.cond) is str:
            self.parent.queried_df = self.df[self.df[self.cond] < eq]
            self.cond = getattr(self.Table, self.cond)
        if type(eq) is str:
            eq = '"' + str(eq) + '"'
        self.parent.query = self.parent.query.filter(
            self.cond < eq)
        return self.parent

    def gt(self, eq):
        if type(self.cond) is str:
            self.parent.queried_df = self.df[self.df[self.cond] > eq]
            self.cond = getattr(self.Table, self.cond)
        if type(eq) is str:
            eq = '"' + str(eq) + '"'
        self.parent.query = self.parent.query.filter(
            self.cond > eq)
        return self.parent
