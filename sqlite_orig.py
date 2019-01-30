import sqlite3
from datetime import datetime


def create_table(connection_str, table_name, record_list, err_values):
    with sqlite3.connect(connection_str) as conn:
        cursor = conn.cursor()
        cursor.execute(f'DROP TABLE IF EXISTS {table_name}');
        cursor.execute(f'CREATE TABLE {table_name} (Key text UNIQUE, Properties TEXT, State INTEGER, '
                       f'CarCount INTEGER, Message TEXT);')

        cursor.executemany(f'INSERT INTO {table_name} (Key, Properties) VALUES(?, ?)', record_list)
        if len(err_values):
            cursor.executemany(f'INSERT INTO {table_name} (Key, Properties, State, Message) VALUES(?, ?, ?, ?)',
                               err_values)


def time_format(data):
    ret = '{}:{:0>2}'.format(int(int(data) / 60 / 60),
                                        int((int(data) - int(int(data) / 60 / 60) * 60 * 60) / 60)) \
        if data.isdigit() else '0:00'
    return ret


def date_format(data, data_default):
    ret = datetime.fromordinal(int(data)+672046).strftime('%Y-%m-%d') if data.isdigit() else data_default
    return ret
