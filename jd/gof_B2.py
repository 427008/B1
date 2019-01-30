from decimal import Decimal, getcontext
from sqlite_orig import create_table, time_format, date_format
from jd.sql_B_jd import putomssql_B2
from jd.sqlite_B_jd import putosqlite_B2
from datetime import datetime


def get_B2(path, date_unload, connection_str, sqlite_conn_str, sqlite_conn2_str):
    print(datetime.now().strftime("%I:%M:%S "), 'B2')
    trash = ["021087;59596064", "021087;59597062", "021087;59806570", "021087;59806877", "021087;59807008",
             "021087;59807271", "021087;59813881", "021087;59814053", "021087;59814525", "021087;59831388",
             "021087;59831743"]
    record_list = []
    record_list_sql = []
    err = {}
    b2 = 'B2_RWBillLine'
    t2000 = datetime(2000, 1, 1).strftime('%Y-%m-%d')
    getcontext().prec = 4
    total = 0
    with open(f'{path}\{date_unload}\Data\B2.gof') as fr:
        i = 0
        for line in fr:
            i = i + 1
            if i < 4 or line[0] == '^' or line == '\n': continue
            record = [r.strip() for r in line.split('~')]
            key = ';'.join([record[0], record[2]])
            if key in trash and record[1] == 'б/н':
                continue

            total += 1
            if len(record) < 10 or record[0] == '' or record[2] == '':
                msg = ''
                if len(record) < 10:
                    msg = 'мало данных %' % len(record)
                elif record[0] == '':
                    msg = 'пусто накладная'
                elif record[2] == '':
                    msg = 'пусто вагон'
                err[i] = (key, ';'.join(record), -1, msg, )
                continue

            while len(record) < 38:
                record.append('')
            rec = ';'.join(record)
            record_list.append((key, rec, ))

            # record[3] & record[4] & record[7] & record[11] & record[24] & record[29] & record[36] == ''
            # record[26] useless
            A30_DateFinish = date_format(record[5], t2000)
            A31_TimeFinish = time_format(record[6])
            A27_SectionNo = int(record[8]) if record[8] else 0
            A63_CargoNetDoc = str(Decimal(record[9])) if record[9] else '0.0'
            A62_CargoNetFact = str(Decimal(record[10])) if record[10] else '0.0'
            A184_NetTotal = str(Decimal(record[13])) if record[13] else '0.0'
            A5_Contract = int(record[14]) if record[14] else 0
            A481_Condition = int(record[20]) if record[20] else 0
            A482_State = int(record[21]) if record[21] else 0
            A601_Shipped = str(Decimal(record[23])) if record[23] else '0.0'
            A603_Spisano = str(Decimal(record[25])) if record[25] else '0.0'
            A624_Carrying = str(Decimal(record[27])) if record[27] else '0.0'
            A563_Weight = str(Decimal(record[30])) if record[30] else '0.0'
            A604_DeclWeight = str(Decimal(record[32])) if record[32] else '0.0'
            A519_ContractNew = int(record[34]) if record[34] else 0
            A4_Owner = int(record[35]) if record[35] else 0
            A190_WeightInContainer = str(Decimal(record[37])) if record[37] else '0.0'

            record_list_sql.append((record[0], record[1], record[2], A30_DateFinish, A31_TimeFinish,
                            A27_SectionNo, A63_CargoNetDoc, A62_CargoNetFact, record[12], A184_NetTotal,
                            A5_Contract, record[15], record[16], record[17],
                            record[18], record[19], A481_Condition, A482_State, record[22], A601_Shipped,
                            A603_Spisano, A624_Carrying, record[28], A563_Weight, record[31], A604_DeclWeight,
                            record[33], A519_ContractNew, A4_Owner, A190_WeightInContainer, ))

    print('B2 total:', total)
    print(datetime.now().strftime("%I:%M:%S "), 'B2 to sqlite', len(record_list), ' errors:', len(err.values()))
    create_table(sqlite_conn_str, b2, record_list, err.values())
    print(datetime.now().strftime("%I:%M:%S "), 'B2 to mssql', len(record_list_sql))
    putomssql_B2(record_list_sql, connection_str)
    print(datetime.now().strftime("%I:%M:%S "), 'B2 to sqlite 2:', len(record_list_sql))
    putosqlite_B2(record_list_sql, sqlite_conn2_str)
    print(datetime.now().strftime("%I:%M:%S "), 'sql - OK')
