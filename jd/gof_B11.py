from sqlite_orig import create_table, time_format, date_format
from jd.sql_B_jd import putomssql_B11
from jd.sqlite_B_jd import putosqlite_B11
from datetime import datetime



def get_B11(path, date_unload, connection_str, sqlite_conn_str, sqlite_conn2_str):
    print(datetime.now().strftime("%I:%M:%S "), 'B11')
    record_list = []
    record_list_sql = []
    err = {}
    b11 = 'B11_RWBillEmpty'
    t2000 = datetime(2000, 1, 1).isoformat()
    total = 0
    with open(f'{path}\{date_unload}\Data\B11.gof') as fr:
        i = 0
        for line in fr:
            i = i + 1
            if i < 4 or line[0] == '^' or line == '\n': continue
            record = [r.strip() for r in line.split('~')]
            if record[0] == '':
                continue

            total += 1
            if len(record) < 10:
                err[i] = (record[0], ';'.join(record), -1, '%s мало данных' % len(record), )
                continue

            while len(record) < 19:
                record.append('')
            rec = ';'.join(record)
            record_list.append((record[0], rec,))

            # record[7] & record[15] == ''
            A1_rwb = int(record[0]) if record[0] else 0
            # ND_rwb (1)
            A9_Date = date_format(record[2], t2000)
            A2_Sender = int(record[3]) if record[3] else 0
            A224_recipient = int(record[4]) if record[4] else 0
            A6_Qty = int(record[5]) if record[5] else 0
            # A474_route (6)
            # A553_info (8)
            # A554_info (9)
            A46_time = time_format(record[10])
            A108_station = int(record[11]) if record[11] else 0
            # A107_recipient_rwb (12)
            # A85_address (13)
            A560_station_from = int(record[14]) if record[14] else 0
            A462_state = int(record[16]) if record[16] else 0
            A66_No = int(record[17]) if record[17] else 0

            record_list_sql.append((A1_rwb, record[1], A9_Date, A2_Sender, A224_recipient, A6_Qty, record[6],
                                    record[8], record[9], A46_time, A108_station, record[12], record[13],
                                    A560_station_from, A462_state, A66_No,))

    print('B11 total:', total)
    print(datetime.now().strftime("%I:%M:%S "), 'B11 to sqlite:', len(record_list), ' errors:', len(err.values()))
    create_table(sqlite_conn_str, b11, record_list, err.values())
    print(datetime.now().strftime("%I:%M:%S "), 'B11 to mssql:', len(record_list_sql))
    putomssql_B11(record_list_sql, connection_str)
    print(datetime.now().strftime("%I:%M:%S "), 'B11 to sqlite 2:', len(record_list_sql))
    putosqlite_B11(record_list_sql, sqlite_conn2_str)
    print(datetime.now().strftime("%I:%M:%S "), 'sql - OK')


#import sqlite3
    # dict_tmp = {}
    # rwb_cars_dict = {}
    # rwb_set = set()
    # rwb_car_set = set()
    #
    # conn = sqlite3.connect(sqlite_conn_str)
    # cursor = conn.cursor()
    # cursor.execute(f'SELECT Key, count(*) FROM {b11s1} GROUP BY Key')
    # for row in cursor.fetchall():
    #     rwb_car_set.add(row[0])
    #     rwb_cars_dict[row[0]] = int(row[1])
    #
    # cursor.execute(f'SELECT id, Key FROM {b11}')
    # for row in cursor.fetchall():
    #     rwb_set.add(row[1])
    #     if row[1] in rwb_cars_dict.keys():
    #         count = rwb_cars_dict[row[1]]
    #     else:
    #         count = 0
    #     dict_tmp[int(row[0])] = (count, int(row[0]), )
    #
    # cursor.executemany(f'UPDATE {b11} SET CarCount=? WHERE id=?', dict_tmp.values())
    # cursor.execute(f'UPDATE {b11} SET State=0 WHERE CarCount=0 and State is NULL')
    # conn.commit()
    # conn.close()
    #
    # for key in rwb_car_set.difference(rwb_set):
    #     print(key)
