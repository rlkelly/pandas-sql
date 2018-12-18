from pandas_sql.base import SqlDataFrame


if __name__ == '__main__':
    first_dataframe = SqlDataFrame(
        {'number': range(5), 'owner': ['al', 'beau', 'chris', 'dan', 'ed']},
        table_name='homes',
    )
    second_dataframe = SqlDataFrame(
        {'house_number': range(5), 'pet': ['cat', 'dog', 'bird', None, None]},
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
