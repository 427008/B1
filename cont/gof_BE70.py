from decimal import getcontext
from sqlite_orig import create_table, date_format, time_format
from cont.sql_B_cont import putomssql_BE7
from cont.sqlite_B_cont import putosqlite_BE7
from datetime import datetime


def get_BE70(path, date_unload, connection_str, sqlite_conn_str, sqlite_conn2_str):
    print(datetime.now().strftime("%I:%M:%S "), 'BE70')
    record_list = []
    record_list_sql = []
    err = {}
    be7 = 'BE70_Zatar'
    trash = []
    t2000 = datetime(2000, 1, 1).strftime('%Y-%m-%d')
    getcontext().prec = 4
    total = 0
    with open(f'{path}\{date_unload}\Data\BE70.gof') as fr:
        i = 0
        for line in fr:
            i = i + 1
            if i < 4 or line[0] == '^' or line == '\n':
                continue

            record = [r.strip() for r in line.split('~')]
            if record[0] == '':
                continue

            if record[0] in trash:
                continue

            total += 1
            # if len(record) < 13:
            #     err[i] = (record[0], ';'.join(record), -1, '%s мало данных' % len(record), )
            #     continue

            while len(record) < 41:
                record.append('')
            rec = ';'.join(record)

            record_list.append((record[0], rec,))

# 11 A296_oper
# 12 A830_zajavka
# 14 A829_Act_plomb
# 19 A953_Act_to_KP
# 22 A956_Act_to_Stek
# 25 A828_tamoj
# 26 A45_Surv
# 34 A1284_lot_no
            id = ' '.join([record[5], record[6]])
            A402_dt = date_format(record[3], t2000)
            A673_tm = time_format(record[4])
            A893_SEZ_dt =  date_format(record[9], t2000)
            A741_state = int(record[13]) if record[13] else 0
            A890_weightNet = int(record[15]) if record[15] else 0
            A771_weightBrtFact = int(record[16]) if record[16] else 0
            A849_Krepl = int(record[17]) if record[17] else 0
            A889_weightPorojn = int(record[18]) if record[18] else 0
            A867_fio_prinial = int(record[24]) if record[24] else 0
            A833_Contract = int(record[27]) if record[27] else 0
            A832_Owner = int(record[28]) if record[28] else 0
            A834_predsavitel = int(record[29]) if record[29] else 0
            A10_gross = int(record[30]) if record[30] else 0
            A863_WesKreplenia = int(record[33]) if record[33] else 0

            AX1 = int(record[35]) if record[35] else 0
            AX2 = int(record[36]) if record[36] else 0
            AX3 = int(record[37]) if record[37] else 0
            AX4 = int(record[38]) if record[38] else 0
            AX5 = int(record[39]) if record[39] else 0
            AX6 = int(record[40]) if record[40] else 0

            record_list_sql.append((id, record[0], A402_dt, A673_tm, record[5], record[6],
                A893_SEZ_dt, record[11], record[12],
                A741_state, record[14], A890_weightNet, A771_weightBrtFact, A849_Krepl, A889_weightPorojn,
                record[19], record[22], A867_fio_prinial,
                record[25], record[26], A833_Contract, A832_Owner, A834_predsavitel, A10_gross,
                A863_WesKreplenia, record[34], AX1, AX2, AX3, AX4, AX5, AX6))

    print('BE70 total:', total)
    print(datetime.now().strftime("%I:%M:%S "), 'BE70 to sqlite:', len(record_list), ' errors:', len(err.values()))
    create_table(sqlite_conn_str, be7, record_list, err.values())
    print(datetime.now().strftime("%I:%M:%S "), 'BE70 to mssql:', len(record_list_sql))
    putomssql_BE7(record_list_sql, connection_str, False)
    print(datetime.now().strftime("%I:%M:%S "), 'BE70 to sqlite 2:', len(record_list_sql))
    putosqlite_BE7(record_list_sql, sqlite_conn2_str, False)
    print(datetime.now().strftime("%I:%M:%S "), 'sql - OK')
