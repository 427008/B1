from decimal import Decimal, getcontext
from sqlite_orig import create_table, date_format
from jd.sql_B_jd import putomssql_B1
from jd.sqlite_B_jd import putosqlite_B1
from datetime import datetime


def get_B1(path, date_unload, connection_str, sqlite_conn_str, sqlite_conn2_str):
    print(datetime.now().strftime("%I:%M:%S "), 'B1')
    record_list = []
    record_list_sql = []
    err = {}
    b1 = 'B1_RWBill'
    t2000 = datetime(2000, 1, 1).strftime('%Y-%m-%d')
    getcontext().prec = 4
    total = 0
    with open(f'{path}\{date_unload}\Data\B1.gof') as fr:
        i = 0
        for line in fr:
            i = i + 1
            if i < 4 or line[0] == '^' or line == '\n':
                continue

            record = [r.strip() for r in line.split('~')]
            if record[0] == '':
                continue

            total += 1
            if len(record) < 15:
                err[i] = (record[0], ';'.join(record), -1, '%s мало данных' % len(record), )
                continue

            while len(record) < 16:
                record.append('')
            rec = ';'.join(record)

            record_list.append((record[0], rec,))

            A473_Route = int(record[1]) if record[1] else 0
            A9_Date = date_format(record[2], t2000)
            A2_Sender = int(record[3]) if record[3] else 0
            A4_Recipient = int(record[4]) if record[4] else 0
            A72_Country = int(record[5]) if record[5] else 0
            A10_Cargo = int(record[6]) if record[6] else 0
            A6_Qty = int(record[7]) if record[7] else 0
            A7_CargoNet = str(Decimal(record[8])) if record[8] else '0.0'
            A17_QtyReceived = int(record[9]) if record[9] else 0
            A54_CargoNetReceived = str(Decimal(record[10])) if record[10] else '0.0'
            A557_Contract = int(record[11]) if record[11] else 0
            A558_Customer = int(record[12]) if record[12] else 0
            A410_Recipient = int(record[14]) if record[14] else 0
            A108_Station = int(record[15]) if record[15] else 0
            record_list_sql.append((record[0], A473_Route, A9_Date, A2_Sender, A4_Recipient, A72_Country, A10_Cargo, A6_Qty,
                           A7_CargoNet, A17_QtyReceived, A54_CargoNetReceived, A557_Contract, A558_Customer, record[13],
                           A410_Recipient, A108_Station,))

    print('B1 total:', total)
    print(datetime.now().strftime("%I:%M:%S "), 'B1 to sqlite:', len(record_list), ' errors:', len(err.values()))
    create_table(sqlite_conn_str, b1, record_list, err.values())
    print(datetime.now().strftime("%I:%M:%S "), 'B1 to mssql:', len(record_list_sql))
    putomssql_B1(record_list_sql, connection_str)
    print(datetime.now().strftime("%I:%M:%S "), 'B1 to sqlite 2:', len(record_list_sql))
    putosqlite_B1(record_list_sql, sqlite_conn2_str)
    print(datetime.now().strftime("%I:%M:%S "), 'sql - OK')

#import sqlite3
    # dict_tmp = {}
    # rwb_cars_dict = {}
    # rwb_set = set()
    # rwb_car_set = set()
    #
    # conn = sqlite3.connect(sqlite_conn_str)
    # cursor = conn.cursor()
    # cursor.execute(f'SELECT Key, count(*) FROM {b2} GROUP BY Key')
    # for row in cursor.fetchall():
    #     rwb_car_set.add(row[0])
    #     rwb_cars_dict[row[0]] = int(row[1])
    #
    # cursor.execute(f'SELECT id, Key FROM {b1}')
    # for row in cursor.fetchall():
    #     rwb_set.add(row[1])
    #     if row[1] in rwb_cars_dict.keys():
    #         count = rwb_cars_dict[row[1]]
    #     else:
    #         count = 0
    #     dict_tmp[int(row[0])] = (count, int(row[0]), )
    #
    # cursor.executemany(f'UPDATE {b1} SET CarCount=? WHERE id=?', dict_tmp.values())
    # cursor.execute(f'UPDATE {b1} SET State=0 WHERE CarCount=0 and State is NULL')
    # conn.commit()
    # conn.close()
    #
    # for key in rwb_car_set.difference(rwb_set):
    #     print(key)
