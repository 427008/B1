from sqlite_orig import create_table
from jd.sql_B_jd import putomssql_B49S2
from jd.sqlite_B_jd import putosqlite_B49S2
from datetime import datetime


def get_B49S2(path, date_unload, connection_str, sqlite_conn_str, sqlite_conn2_str):
    print(datetime.now().strftime("%I:%M:%S "), 'B49S2')
    record_list = []
    record_list_sql = []
    err = {}
    b49_2 = 'B49S2_Route_Notice'
    t2000 = datetime(2000, 1, 1).isoformat()
    total = 0
    with open(f'{path}\{date_unload}\Data\B49S2.gof') as fr:
        i = 0
        for line in fr:
            i = i + 1
            if i < 4 or line[0] == '^' or line == '\n': continue
            record = [r.strip() for r in line.split('~')]
            if record[0] == '':
                continue

            total += 1
            if len(record) < 5:
                err[i] = (record[0], ';'.join(record), -1, '%s мало данных' % len(record), )
                continue

            rec = ';'.join(record)
            record_list.append((rec, rec,))

            # 64951.9~2871~1~ЭЯ974445~90214792
            # A474	Номер маршрута
            # A561	Номер памятки
            # A562	Номер уведомления
            # A1		Номер основной жд накладной
            # A58		Номер жд вагона

            record_list_sql.append((record[0], record[1], record[2], record[3], record[4],))

    print('B49S2 total:', total)
    print(datetime.now().strftime("%I:%M:%S "), 'B49S2 to sqlite:', len(record_list), ' errors:', len(err.values()))
    create_table(sqlite_conn_str, b49_2, record_list, err.values())
    print(datetime.now().strftime("%I:%M:%S "), 'B49S2 to mssql:', len(record_list_sql))
    putomssql_B49S2(record_list_sql, connection_str)
    print(datetime.now().strftime("%I:%M:%S "), 'B49S2 to sqlite 2:', len(record_list_sql))
    putosqlite_B49S2(record_list_sql, sqlite_conn2_str)
    print(datetime.now().strftime("%I:%M:%S "), 'sql - OK')

