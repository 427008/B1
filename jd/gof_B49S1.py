from sqlite_orig import create_table, time_format, date_format
from jd.sql_B_jd import putomssql_B49S1
from jd.sqlite_B_jd import putosqlite_B49S1
from datetime import datetime


def get_B49S1(path, date_unload, connection_str, sqlite_conn_str, sqlite_conn2_str):
    print(datetime.now().strftime("%I:%M:%S "), 'B49S2')
    record_list = []
    record_list_sql = []
    err = {}
    b49_1 = 'B49S1_Route_Notice_Time'
    t2000 = datetime(2000, 1, 1).isoformat()
    total = 0
    with open(f'{path}\{date_unload}\Data\B49S1.gof') as fr:
        i = 0
        for line in fr:
            i = i + 1
            if i < 4 or line[0] == '^' or line == '\n': continue
            record = [r.strip() for r in line.split('~')]
            if record[0] == '':
                continue

            total += 1
            if len(record) < 6:
                err[i] = (record[0], ';'.join(record), -1, '%s мало данных' % len(record), )
                continue

            while len(record) < 9:
                record.append('')
            rec = ';'.join(record)
            record_list.append((rec, rec,))

            # A474_route
            # A561_memo
            # A562_notice
            # A556_path_no
            A28_date_in = date_format(record[4], t2000)
            A29_time_in = time_format(record[5])
            A30_date_end = date_format(record[6], t2000)
            A31_time_end = time_format(record[7])

            record_list_sql.append((record[0], record[1], record[2], record[3],
                                    A28_date_in, A29_time_in, A30_date_end, A31_time_end,))

    print('B49S1 total:', total)
    print(datetime.now().strftime("%I:%M:%S "), 'B49S1 to sqlite:', len(record_list), ' errors:', len(err.values()))
    create_table(sqlite_conn_str, b49_1, record_list, err.values())
    print(datetime.now().strftime("%I:%M:%S "), 'B49S1 to mssql:', len(record_list_sql))
    putomssql_B49S1(record_list_sql, connection_str)
    print(datetime.now().strftime("%I:%M:%S "), 'B49S1 to sqlite 2:', len(record_list_sql))
    putosqlite_B49S1(record_list_sql, sqlite_conn2_str)
    print(datetime.now().strftime("%I:%M:%S "), 'sql - OK')

