from sqlite_orig import create_table, date_format, time_format
from cont.sql_B_cont import putomssql_BE1
from cont.sqlite_B_cont import putosqlite_BE1
from datetime import datetime


def get_BE1(path, date_unload, connection_str, sqlite_conn_str, sqlite_conn2_str):
    print(datetime.now().strftime("%I:%M:%S "), 'BE1')
    record_list = []
    record_list_sql = []
    err = {}
    be5 = 'BE1_zajav'
    t2000 = datetime(2000, 1, 1).strftime('%Y-%m-%d')
    total = 0
    with open(f'{path}\{date_unload}\Data\BE1.gof') as fr:
        i = 0
        for line in fr:
            i = i + 1
            if i < 4 or line[0] == '^' or line == '\n':
                continue

            record = [r.strip() for r in line.split('~')]
            if record[0] == '':
                continue

            total += 1
            if len(record) < 13:
                err[i] = (record[0], ';'.join(record), -1, '%s мало данных' % len(record), )
                continue

            while len(record) < 16:
                record.append('')
            rec = ';'.join(record)

            record_list.append((record[0], rec,))

            A1001_dt = date_format(record[2], t2000)
            A832_Owner = int(record[3]) if record[3] else 0
            A833_Contract = int(record[4]) if record[4] else 0
            A367_dt_n = date_format(record[5], t2000)
            A673_tm_n = time_format(record[6])
            A452_dt_k = date_format(record[7], t2000)
            A642_tm_k = time_format(record[8])
            A738_state = int(record[11]) if record[11] else 0
            A834_predsavitel = int(record[12]) if record[12] else 0

            record_list_sql.append((record[0], record[1], A1001_dt, A832_Owner, A833_Contract,
                                    A367_dt_n, A673_tm_n, A452_dt_k, A642_tm_k, record[10], A738_state,
                                    A834_predsavitel, record[13], record[14], record[15], ))

    print('BE1 total:', total)
    print(datetime.now().strftime("%I:%M:%S "), 'BE1 to sqlite:', len(record_list), ' errors:', len(err.values()))
    create_table(sqlite_conn_str, be5, record_list, err.values())
    print(datetime.now().strftime("%I:%M:%S "), 'BE1 to mssql:', len(record_list_sql))
    putomssql_BE1(record_list_sql, connection_str)
    print(datetime.now().strftime("%I:%M:%S "), 'BE1 to sqlite 2:', len(record_list_sql))
    putosqlite_BE1(record_list_sql, sqlite_conn2_str)
    print(datetime.now().strftime("%I:%M:%S "), 'sql - OK')
