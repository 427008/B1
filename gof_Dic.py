from sqlite_orig import create_table
from datetime import datetime
from sql_B import putomssql_dic
from sqlite_B import putosqlite_dic


def get_dict(path, date_unload, fn, connection_str, sqlite_conn_str, sqlite_conn2_str):
    print(datetime.now().strftime("%I:%M "), fn)
    total = 0
    with open(f'{path}\{date_unload}\Data\{fn}.gof') as fr:
        i = 0
        record_list = []
        record_list_sql = []
        dict_tmp = {}
        err = {}
        for line in fr:
            i = i + 1
            if i < 4 or line[0] == '^' or line == '\n': continue
            record = [r.strip() for r in line.split('~')]

            total += 1
            if len(record) < 3:
                err[i] = [i, line]
                continue

            while len(record) < 4:
                record.append('')

            record_list.append((record[0], ';'.join(record[0:3]),))

            id = int(record[0]) if record[0] else 0

            record_list_sql.append((id, record[1], record[2],))

    print(fn, ' total:', total)
    print(datetime.now().strftime("%I:%M:%S "), 'to sqlite:', len(record_list), ' errors:', len(err.values()))
    create_table(sqlite_conn_str, fn, record_list, err.values())
    print(datetime.now().strftime("%I:%M:%S "), 'to mssql:', len(record_list_sql))
    putomssql_dic(record_list_sql, connection_str, fn)
    print(datetime.now().strftime("%I:%M:%S "), 'to sqlite 2:', len(record_list_sql))
    putosqlite_dic(record_list_sql, sqlite_conn2_str, fn)
    print(datetime.now().strftime("%I:%M:%S "), 'sql - OK')
