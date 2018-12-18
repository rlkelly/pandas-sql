import pandas as pd


class Where(object):
    def __init__(self, df, cond):
        self.df = df
        self.cond = cond
    def eq(self, eq):
        return self.df[self.df[self.cond] == eq]

class DataFrame(object):
    def __init__(self, df):
        self.df = df
    def select(self, col):
        self.df = self.df[col]
        return self
    def where(self, col):
        self.df = Where(self.df, col)
        return self
    def eq(self, eq):
        self.df = self.df.eq(eq)
        return self

if __name__ == '__main__':
    a = DataFrame(pd.DataFrame({'a': range(3), 'b': range(3)}))
    a.select(['a', 'b']).where('a').eq(1)
