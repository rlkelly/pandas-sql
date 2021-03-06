# pandas-sql

This attempts to allow users to code in pandas but generate SQL queries.  To run:
      python setup.py install

An example might look like:

    from sql_pandas import SqlDataFrame

    first_dataframe = SqlDataFrame(
        {'number': range(5), 'owner': ['al', 'beau', 'chris', 'dan', 'ed']},
        table_name='homes',
    )
    second_dataframe = SqlDataFrame(
        {'house_number': range(5), 'pet': ['cat', 'dog', 'bird', None, None]},
        table_name='pets',
    )
    print(
        first_dataframe[[first_dataframe.number, second_dataframe.pet]]
            .where('owner').eq('al')
            .innerjoin(
                second_dataframe,
                first_dataframe.number,
                second_dataframe.house_number,
            )
            .collect()
            .extract_query()
    )

This will build a query while updating the dataframe.  Once you complete your query you can call `collect()` to update the internal dataframe, or `reset()` to reset your queried dataframe to the original.
Then you can call `extract_query()` to return the sql_string to build your query.  Supports all dialects from SqlAlchemy Core.
