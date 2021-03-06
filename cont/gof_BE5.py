from sqlite_orig import create_table, date_format, time_format
from cont.sql_B_cont import putomssql_BE5
from cont.sqlite_B_cont import putosqlite_BE5
from datetime import datetime


def get_BE5(path, date_unload, connection_str, sqlite_conn_str, sqlite_conn2_str):
    print(datetime.now().strftime("%I:%M:%S "), 'BE5')
    record_list = []
    record_list_sql = []
    total_dict = []
    err = {}
    be5 = 'BE5_Priem'
    t2000 = datetime(2000, 1, 1).strftime('%Y-%m-%d')
    total = 0
    record7065 = []
    with open(f'{path}\{date_unload}\Data\BE5.gof') as fr:
        i = 0
        for line in fr:
            i = i + 1
            if i < 4:
                continue
            if line[0] == '^' or line == '\n':
                continue

            record = [r.strip() for r in line.split('~')]
            if record[0] == '18/94/7065':
                record7065 = record
                continue
            if len(record7065) > 0:
                record = record7065 + record[1:]
                record7065 = []

            if not record[0][0:2].isdigit() or (int(record[0][0:2]) < 10 and int(record[0][0:2]) > 18):
                print(line)
            if record[0] == '':
                continue

            total += 1
            if len(record) < 13:
                err[i] = (record[0], ';'.join(record), -1, '%s мало данных' % len(record), )
                continue

            while len(record) < 15:
                record.append('')
            rec = ';'.join(record)

            record_list.append((record[0], rec,))

            id = ' '.join([record[3], record[4]])
            A402_dt = date_format(record[1], t2000)
            A673_tm = time_format(record[2])
            A866_fio_cdal = int(record[5]) if record[5] else 0
            A867_fio_prinial = int(record[6]) if record[6] else 0
            A0 = int(record[14]) if record[14] else 0
            record_list_sql.append((id, record[0], A402_dt, A673_tm, record[3], record[4], A866_fio_cdal, A867_fio_prinial,
                                    record[7], record[8], record[10], record[11], A0, ))

    print('BE5 total:', total)
    print(datetime.now().strftime("%I:%M:%S "), 'BE5 to sqlite:', len(record_list), ' errors:', len(err.values()))
    create_table(sqlite_conn_str, be5, record_list, err.values())
    print(datetime.now().strftime("%I:%M:%S "), 'BE5 to mssql:', len(record_list_sql))
    putomssql_BE5(record_list_sql, connection_str)
    print(datetime.now().strftime("%I:%M:%S "), 'BE5 to sqlite 2:', len(record_list_sql))
    putosqlite_BE5(record_list_sql, sqlite_conn2_str)
    print(datetime.now().strftime("%I:%M:%S "), 'sql - OK')
