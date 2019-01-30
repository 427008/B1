from sqlite_orig import create_table, time_format, date_format
from jd.sql_B_jd import putomssql_B59
from jd.sqlite_B_jd import putosqlite_B59
from datetime import datetime


def get_B59(path, date_unload, connection_str, sqlite_conn_str, sqlite_conn2_str):
    print(datetime.now().strftime("%I:%M:%S "), 'B59')
    record_list = []
    record_list_sql = []
    err = {}
    b59 = 'B59_Time'
    t2000 = datetime(2000,1,1).strftime('%Y-%m-%d')
    total = 0
    with open(f'{path}\{date_unload}\Data\B59.gof') as fr:
        i = 0
        for line in fr:
            i = i + 1
            if i < 4 or line[0] == '^' or line == '\n':
                continue

            record = [r.strip() for r in line.split('~')]
            if record[0] == '':
                continue

            total += 1
            key = ';'.join([record[0], record[1]])
            if len(record) < 35 or record[0] == '' or record[1] == '':
                msg = ''
                if len(record) < 35:
                    msg = '%s мало данных' % len(record)
                elif record[0] == '':
                    msg = 'пусто накладная'
                elif record[1] == '':
                    msg = 'пусто вагон'
                err[i] = (key, ';'.join(record), -1, msg, )
                continue

            while len(record) < 52:
                record.append('')
            rec = ';'.join(record)
            record_list.append((key, rec, ))

            # record[31] & record[48] == ''
            A367_DRaskred = date_format(record[2], t2000)
            A122_TRaskred = time_format(record[3])
            A452_DVruchPam = date_format(record[4], t2000)
            A120_TVruchPam = time_format(record[5])
            A261_DFactGet = date_format(record[6], t2000)
            A31_TFactGet = time_format(record[7])

            A37_DCheck = date_format(record[8], t2000)
            A271_TCheck = time_format(record[9])
            A402_DPereDekl = date_format(record[10], t2000)
            A29_TPereDekl = time_format(record[11])
            A321_DUnload = date_format(record[12], t2000)
            A642_TUnload = time_format(record[13])
            A21_DPostCheck = date_format(record[14], t2000)
            A643_TPostCheck = time_format(record[15])
            A441_DOtstavka = date_format(record[16], t2000)
            A644_TOtstavka = time_format(record[17])
            A324_DIzOtsavka = date_format(record[18], t2000)
            A645_TIzOtsavka = time_format(record[19])

            A278_DPA = date_format(record[20], t2000)
            A646_TPA = time_format(record[21])
            A121_DNoticeOfUnload = date_format(record[22], t2000)
            A647_TNoticeOfUnload = time_format(record[23])
            A358_DJDNBack = date_format(record[24], t2000)
            A648_TJDNBack = time_format(record[25])
            A323_DFactBack = date_format(record[26], t2000)
            A649_TFactBack = time_format(record[27])
            A650_DForwarding = date_format(record[28], t2000)
            A651_TForwarding = time_format(record[29])

            A652_DPamiatkaZakr = date_format(record[36], t2000)
            A653_TPamiatkaZakr = time_format(record[37])
            A656_DPeresilPoKvit = date_format(record[38], t2000)
            A657_TPeresilPoKvit = time_format(record[39])

            A268_DPereosvid = date_format(record[42], t2000)
            A269_TPereosvid = time_format(record[43])
            A9_DOtprav = date_format(record[44], t2000)

            A28_DPredvSpis = date_format(record[45], t2000)
            A46_TPredvSpis = time_format(record[46])

            A52_DPA_MKR = date_format(record[49], t2000)
            A226_TPA_MKR = time_format(record[50])

            record_list_sql.append((record[0], record[1], A367_DRaskred, A122_TRaskred,
                                    A452_DVruchPam, A120_TVruchPam, A261_DFactGet, A31_TFactGet,
                                    A37_DCheck, A271_TCheck, A402_DPereDekl, A29_TPereDekl,
                                    A321_DUnload, A642_TUnload, A21_DPostCheck, A643_TPostCheck,
                                    A441_DOtstavka, A644_TOtstavka, A324_DIzOtsavka, A645_TIzOtsavka,
                                    A278_DPA, A646_TPA, A121_DNoticeOfUnload, A647_TNoticeOfUnload,
                                    A358_DJDNBack, A648_TJDNBack, A323_DFactBack, A649_TFactBack,
                                    A650_DForwarding, A651_TForwarding,
                                    record[30], record[32], record[33], record[34], record[35],
                                    A652_DPamiatkaZakr, A653_TPamiatkaZakr, A656_DPeresilPoKvit, A657_TPeresilPoKvit,
                                    record[40], record[41], A268_DPereosvid, A269_TPereosvid,
                                    A9_DOtprav, A28_DPredvSpis, A46_TPredvSpis,
                                    record[47], A52_DPA_MKR, A226_TPA_MKR, ))

    print('B59 total:', total)
    print(datetime.now().strftime("%I:%M:%S "), 'B59 to sqlite:', len(record_list), ' errors:', len(err.values()))
    create_table(sqlite_conn_str, b59, record_list, err.values())
    print(datetime.now().strftime("%I:%M:%S "), 'B59 to mssql:', len(record_list_sql))
    putomssql_B59(record_list_sql, connection_str)
    print(datetime.now().strftime("%I:%M:%S "), 'B59 to sqlite 2:', len(record_list_sql))
    putosqlite_B59(record_list_sql, sqlite_conn2_str)
    print(datetime.now().strftime("%I:%M:%S "), 'sql - OK')
