from sqlite_orig import create_table, date_format, time_format
from cont.sql_B_cont import putomssql_BE7S1
from cont.sqlite_B_cont import putosqlite_BE7S1
from datetime import datetime


def get_BE70S1(path, date_unload, connection_str, sqlite_conn_str, sqlite_conn2_str):
    print(datetime.now().strftime("%I:%M:%S "), 'BE70S1')
    record_list = []
    record_list_sql = []
    err = {}
    be70S1 = 'BE70S1_JD'
    t2000 = datetime(2000, 1, 1).strftime('%Y-%m-%d')
    total = 0
    with open(f'{path}\{date_unload}\Data\BE70S1.gof') as fr:
        i = 0
        for line in fr:
            i = i + 1
            if i < 4 or line[0] == '^' or line == '\n':
                continue

            record = [r.strip() for r in line.split('~')]
            if record[0] == '':
                continue

            key = ';'.join([record[0], record[1], record[2]])

            total += 1
            if len(record) < 7:
                err[i] = (key, ';'.join(record), -1, '%s мало данных' % len(record), )
                continue

            while len(record) < 7:
                record.append('')
            rec = ';'.join(record)

            record_list.append((key, rec,))

            AX1 = int(record[5]) if record[5] else 0
            A452_dt = date_format(record[6], t2000)
            A642_tm = time_format(record[7])
            record_list_sql.append((record[0], record[1], record[2], record[3], record[4], AX1, A452_dt, A642_tm, ))

    print('BE70S1 total:', total)
    print(datetime.now().strftime("%I:%M:%S "), 'BE70S1 to sqlite:', len(record_list), ' errors:', len(err.values()))
    create_table(sqlite_conn_str, be70S1, record_list, err.values())
    print(datetime.now().strftime("%I:%M:%S "), 'BE70S1 to mssql:', len(record_list_sql))
    putomssql_BE7S1(record_list_sql, connection_str, False)
    print(datetime.now().strftime("%I:%M:%S "), 'BE70S1 to sqlite 2:', len(record_list_sql))
    putosqlite_BE7S1(record_list_sql, sqlite_conn2_str, False)
    print(datetime.now().strftime("%I:%M:%S "), 'sql - OK')
