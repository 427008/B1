from sqlite_orig import create_table
# from jd.sql_B_jd import putomssql_B12A144
# from jd.sqlite_B_jd import putosqlite_B12A144
from datetime import datetime


def get_B12A144(path, date_unload, connection_str, sqlite_conn_str, sqlite_conn2_str):
    return

    print(datetime.now().strftime("%I:%M:%S "), 'B12A144')
    record_list = []
    record_list_sql = []
    err = {}
    b12a144 = 'B12A144_gtd'
    total = 0
    with open(f'{path}\{date_unload}\Data\B12A144.gof') as fr:
        i = 0
        for line in fr:
            i = i + 1
            if i < 4 or line[0] == '^' or line == '\n': continue
            record = [r.strip() for r in line.split('~')]
            if len(record) < 3 or record[1] == '' or record[2] == '':
                msg = ''
                if len(record) < 2:
                    msg = '%s мало данных' % len(record)
                elif record[1] == '':
                    msg = 'пусто накладная'
                elif record[2] == '':
                    msg = 'пусто вагон'
                err[i] = (key, ';'.join(record), -1, msg, )
                continue

            key = ';'.join([record[0], record[1], record[2]])
            total += 1
            record_list.append((key, key,))

            record_list_sql.append((record[0], record[1], record[2],))

    print('B12A144 total:', total)
    print(datetime.now().strftime("%I:%M:%S "), 'B12A144 to sqlite', len(record_list), ' errors:', len(err.values()))
    create_table(sqlite_conn_str, b12a144, record_list, err.values())
    print(datetime.now().strftime("%I:%M:%S "), 'B12A144 to mssql:', len(record_list_sql))
    # putomssql_B12A144(record_list_sql, connection_str)
    print(datetime.now().strftime("%I:%M:%S "), 'B12A144 to sqlite 2:', len(record_list_sql))
    # putosqlite_B12A144(record_list_sql, sqlite_conn2_str)
    print(datetime.now().strftime("%I:%M:%S "), 'sql - OK')
