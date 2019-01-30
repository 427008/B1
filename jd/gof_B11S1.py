from sqlite_orig import create_table
from jd.sql_B_jd import putomssql_B11S1
from jd.sqlite_B_jd import putosqlite_B11S1
from datetime import datetime


def get_B11S1(path, date_unload, connection_str, sqlite_conn_str, sqlite_conn2_str):
    print(datetime.now().strftime("%I:%M:%S "), 'B11S1')
    record_list = []
    record_list_sql = []
    err = {}
    b11s1 = 'B11S1_RWBillEmptyLine'
    total = 0
    with open(f'{path}\{date_unload}\Data\B11S1.gof') as fr:
        i = 0
        for line in fr:
            i = i + 1
            if i < 4 or line[0] == '^' or line == '\n': continue
            record = [r.strip() for r in line.split('~')]
            key = ';'.join([record[0], record[1]])
            if len(record) < 2 or record[0] == '' or record[1] == '':
                msg = ''
                if len(record) < 2:
                    msg = '%s мало данных' % len(record)
                elif record[0] == '':
                    msg = 'пусто накладная'
                elif record[1] == '':
                    msg = 'пусто вагон'
                err[i] = (key, ';'.join(record), -1, msg, )
                continue

            total += 1
            while len(record) < 7:
                record.append('')
            rec = ';'.join(record)
            record_list.append((key, rec,))

            A1_rwb = int(record[0]) if record[0] else 0
            A474_orderNo = int(record[2]) if record[2] else 0
            # A473_route, A65_doc, A563_weght
            record_list_sql.append((A1_rwb, record[1], A474_orderNo, record[3], record[4], record[5],))

    print('B11S1 total:', total)
    print(datetime.now().strftime("%I:%M:%S "), 'B11S1 to sqlite', len(record_list), ' errors:', len(err.values()))
    create_table(sqlite_conn_str, b11s1, record_list, err.values())
    print(datetime.now().strftime("%I:%M:%S "), 'B11S1 to mssql:', len(record_list_sql))
    putomssql_B11S1(record_list_sql, connection_str)
    print(datetime.now().strftime("%I:%M:%S "), 'B11S1 to sqlite 2:', len(record_list_sql))
    putosqlite_B11S1(record_list_sql, sqlite_conn2_str)
    print(datetime.now().strftime("%I:%M:%S "), 'sql - OK')
