import csv
import sqlite3

from models import DataBaseTableColumn


class CSVReader:
    def __init__(self, path_to_file: str = None, separator: str = ';'):
        self.path_to_file = path_to_file
        self.separator = separator

    def get_dict_csv(self, path_to_file: str = None) -> dict:
        path_to_file = self.path_to_file if path_to_file is None else path_to_file

        csv_dict_data = {'columns': list(),
                         'values': list()}
        with open(path_to_file, newline='', encoding='utf-8') as csvfile:
            raw_data = csv.reader(csvfile, delimiter=self.separator)
            column_names = next(raw_data)
            csv_dict_data['columns'] = column_names

            for row in raw_data:
                csv_dict_data['values'].append(row)

        return csv_dict_data


class CSVProccessor:
    def __init__(self):
        pass

    def get_merged_csv(self, path_to_main_csv: str, path_to_additional_csv: str,
                       matching_column_index: int = 0):

        csv_reader = CSVReader()
        main_csv_file = csv_reader.get_dict_csv(path_to_main_csv)
        additional_csv_file = csv_reader.get_dict_csv(path_to_additional_csv)

        additional_csv_lookup = self.get_lookup(additional_csv_file, matching_column_index)
        additional_columns = self.get_additional_columns(additional_csv_file, matching_column_index)

        merged_csv_dict = {'columns': main_csv_file['columns'] + additional_columns,
                           'values': list()}

        for row in main_csv_file['values']:
            matching_value = row[matching_column_index]
            additional_values = additional_csv_lookup.get(matching_value,
                                                          [None for _ in range(len(additional_columns))])
            merged_row = row.copy()
            merged_row.extend(additional_values)
            merged_csv_dict['values'].append(merged_row)

        return merged_csv_dict

    @staticmethod
    def get_lookup(csv_dict_data: dict, column_index: int) -> dict:
        csv_lookup = dict()
        for row in csv_dict_data['values']:
            key = None
            values = list()
            for index, row_element in enumerate(row):
                if index == column_index:
                    key = row_element
                else:
                    values.append(row_element)

            if not key:
                raise ValueError('Csv additional file format incorrect')

            csv_lookup[key] = values

        return csv_lookup

    @staticmethod
    def get_additional_columns(additional_csv_dict: dict, matching_column_index: int):
        additional_columns = list()
        for index, column in enumerate(additional_csv_dict['columns']):
            if index != matching_column_index:
                additional_columns.append(column)

        return additional_columns


class DataBase:
    def __init__(self, database_name: str = 'example.db'):
        self.database_name = database_name
        self.connection = sqlite3.connect(database_name)
        self.cursor = self.connection.cursor()

    def create_table(self, table_name: str, columns: list[DataBaseTableColumn] = None):
        prepared_query = f'''CREATE TABLE IF NOT EXISTS {table_name} 
                  {self.convert_columns_into_query(columns)}'''
        self.cursor.execute(prepared_query)
        return prepared_query

    def convert_columns_into_query(self, columns: list[DataBaseTableColumn] = None):
        if not columns:
            columns = self.get_mock_table_columns()

        query_str = '('
        columns_str_list = list()
        for column in columns:
            str_pres = f'{column.column_name} {column.column_type.upper()}'
            if column.primary_key:
                str_pres += ' PRIMARY KEY'

            columns_str_list.append(str_pres)

        query_str += ', '.join(columns_str_list) + ')'
        return query_str

    def upsert_values(self, table_name: str, columns_names: list, values: list[tuple]):
        prepared_query = f"INSERT INTO {table_name} ({', '.join(columns_names)}) VALUES ({', '.join(['?' for _ in columns_names])})"
        print(prepared_query)
        self.cursor.executemany(prepared_query, values)
        return prepared_query

    def commit_changes(self):
        self.connection.commit()

    @staticmethod
    def get_mock_table_columns():
        return [DataBaseTableColumn(**{'column_name': 'Бренд',
                                       'column_type': 'TEXT'}),

                DataBaseTableColumn(**{'column_name': 'Артикул',
                                       'column_type': 'TEXT'}),

                DataBaseTableColumn(**{'column_name': 'Цена',
                                       'column_type': 'REAL'}),

                DataBaseTableColumn(**{'column_name': 'Param',
                                       'column_type': 'TEXT'})
                ]


def main(path_to_main_csv_file: str,
         path_to_additional_csv_file: str,
         db_name: str = 'example.db',
         db_table_name: str = 'csv_data'):

    merged_csv_dict = CSVProccessor().get_merged_csv(path_to_main_csv=path_to_main_csv_file,
                                                     path_to_additional_csv=path_to_additional_csv_file)

    db = DataBase(database_name=db_name)
    db.create_table(table_name=db_table_name)

    db.upsert_values(table_name=db_table_name,
                     columns_names=merged_csv_dict['columns'],
                     values=[tuple(value) for value in merged_csv_dict['values']])
    db.commit_changes()


if __name__ == '__main__':

    main(path_to_main_csv_file='3.csv',
         path_to_additional_csv_file='2.csv')
