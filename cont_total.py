import sqlite3
import pyodbc
from datetime import datetime, timedelta


def _execute(values, cn, cur, command):
    for i in range(int(len(values) / 1000) + 1):
        cur.executemany(command, values[i * 1000:(i + 1) * 1000])
        cn.commit()


def create_total(path, unloaded, connection_str, sqlite_conn_str):
    print(datetime.now().strftime("%I:%M:%S "), 'total')
    values = []
    with sqlite3.connect(sqlite_conn_str) as conn:
        cursor = conn.cursor()
        cursor.execute("""SELECT cont_priem.id, cont_priem.A728_Act_priem, cont_priem.A402_dt as d_priem
	    ,cont_zatar.A876_Act_zatar, cont_zatar.A402_dt as d_zatar, cont_zatar.A890_weightNet
	    ,case when cont_zatar.A1284_lot_no is NULL or cont_zatar.A1284_lot_no = '' 
	    then cont_zajav.A1284_lot_no else cont_zatar.A1284_lot_no end as A1284_lot_no
        FROM cont_priem 
        LEFT JOIN cont_zatar ON cont_priem.id = cont_zatar.id and cont_priem.A830_zajavka = cont_zatar.A830_zajavka 
		LEFT JOIN cont_zajav ON cont_priem.A830_zajavka = cont_zajav.A830_Zajav
        order by cont_priem.A402_dt desc, cont_priem.A728_Act_priem desc""")
        priem_dict = {row[1]: row for row in cursor.fetchall()}
        priem_back = {}
        for key in priem_dict.keys():
            id = priem_dict[key][0]
            if id in priem_back.keys():
                priem_back[id].add(key)
            else:
                priem_back[id] = {key}

        cursor.execute('SELECT id, A752_Act_out, A402_dt, A746_book_no FROM cont_vivoz order by A402_dt desc')
        vivoz_dict = {row[1]: row for row in cursor.fetchall()}
        vivoz_back = {}
        for key in vivoz_dict.keys():
            id = vivoz_dict[key][0]
            if id in vivoz_back.keys():
                vivoz_back[id].add(key)
            else:
                vivoz_back[id] = {key}

        priem_keys = []
        priem_ids = set()
        vivoz_keys = []
        vivoz_ids = set()
        priem_q = {}
        for key in priem_dict.keys():
            id = priem_dict[key][0]
            if len(priem_back[id]) == 1:
                # пришел 1 раз
                if id in vivoz_back.keys():
                    # вывозился
                    if len(vivoz_back[id]) == 1:
                        # пришел и ушел 1 раз
                        tmp = vivoz_dict[list(vivoz_back[id])[0]]
                        values.append(priem_dict[key] + (tmp[1], tmp[2], tmp[3],))
                        priem_keys.append(key)
                        priem_ids.add(id)
                        vivoz_keys.append(tmp[1])
                        vivoz_ids.add(id)
                    # else: # пришел 1 раз - ушел 2.. !!

            if id not in vivoz_back.keys():
                # не вывозился
                values.append(priem_dict[key] + (None, None, None,))
                priem_q[key] = priem_dict[key]
                priem_keys.append(key)
                priem_ids.add(id)

        for key in priem_keys:
            del(priem_dict[key])
        for id in priem_ids:
            del(priem_back[id])

        for key in vivoz_keys:
            del(vivoz_dict[key])
        for id in vivoz_ids:
            del(vivoz_back[id])

        values1 = []
        for id in priem_back.keys():
            counter = 0
            while len(priem_back[id]) > 0:
                date_dif = timedelta(days=10000)
                key_out = None
                key_in = None
                for key in priem_back[id]:
                    if key not in priem_dict.keys():
                        break # уже сопоставлено
                    if id not in vivoz_back.keys():
                        key_in = key
                        break # не вывезено

                    date_priem = datetime.strptime(priem_dict[key][2], '%Y-%m-%d')
                    date_zatar = date_priem if priem_dict[key][4] is None \
                        else datetime.strptime(priem_dict[key][4], '%Y-%m-%d')
                    for key_back in vivoz_back[id]:
                        date_vivoz = datetime.strptime(vivoz_dict[key_back][2], '%Y-%m-%d')
                        if date_dif > abs(date_vivoz - date_zatar):
                            date_dif = abs(date_vivoz - date_zatar)
                            key_out = vivoz_dict[key_back][1]
                            key_in = key
                        if date_dif > abs(date_vivoz - date_priem):
                            date_dif = abs(date_vivoz - date_priem)
                            key_out = vivoz_dict[key_back][1]
                            key_in = key

                if key_out is None or key_in is None:
                    tmp = (None, None, None, None, )
                else:
                    tmp = vivoz_dict[key_out]

                values.append(priem_dict[key_in] + (tmp[1], tmp[2], tmp[3],))
                del(priem_dict[key_in])
                priem_back[id].remove(key_in)
                if key_out is not None:
                    del(vivoz_dict[key_out])
                    vivoz_back[id].remove(key_out)
                    if len(vivoz_back[id]) == 0:
                        del(vivoz_back[id])

        with open(f'{path}\{unloaded}\Data\in.csv', 'w') as f:
            f.writelines([';'.join([str(item) for item in value]) + '\n' for value in priem_dict.values()])

        with open(f'{path}\{unloaded}\Data\out.csv', 'w') as f:
            f.writelines([';'.join([str(item) for item in value]) + '\n' for value in vivoz_dict.values()])

        #print(priem_back)
        #print(vivoz_back)

        values_jd = {}
        cursor.execute('SELECT A876_Act, A1_rwb, A58_CarNo, A890_weightNet FROM cont_jd order by A452_dt desc')
        for row in cursor.fetchall():
            tmp = (row[1], row[2], row[3],)
            if row[0] in values_jd.keys():
                values_jd[row[0]].append(tmp)
            else:
                values_jd[row[0]] = [tmp]

        tmp = []
        for line in values:
            if line[3] in values_jd.keys():
                if len(values_jd[line[3]]) == 1:
                    tmp.append(line + values_jd[line[3]][0] + (None, None, None, ))
                else:
                    tmp.append(line + values_jd[line[3]][0] + values_jd[line[3]][1])
            else:
                tmp.append(line + (None, None, None, None, None, None, ))

        values = tmp

        cursor.execute('DROP TABLE IF EXISTS cont_total')
        cursor.execute("""CREATE TABLE cont_total (id TEXT, A728_Act_priem TEXT, A402_dt_priem TEXT, 
            A876_Act_zatar TEXT, A402_dt_zatar TEXT,  A890_weightNet INTEGER,  A1284_lot_no TEXT, 
            A752_Act_out TEXT, A402_dt_vivoz TEXT, A746_book_no TEXT, 
            A1_rwb1 TEXT, A58_CarNo1 TEXT, A890_weightNet1 TEXT,
            A1_rwb2 TEXT, A58_CarNo2 TEXT, A890_weightNet2 TEXT)""")
        conn.commit()

        _execute(values, conn, cursor,
                 """INSERT INTO cont_total 
                 (id, A728_Act_priem, A402_dt_priem,  A876_Act_zatar, A402_dt_zatar, A890_weightNet,
                 A1284_lot_no, A752_Act_out, A402_dt_vivoz, A746_book_no, 
                 A1_rwb1, A58_CarNo1, A890_weightNet1, A1_rwb2, A58_CarNo2, A890_weightNet2)
                 VALUES (?, ?, ?, ?, ?, ?,  ?, ?, ?, ?,  ?, ?, ?, ?, ?, ? )
                 """)

        cursor.execute('UPDATE cont_total SET id=REPLACE (id, " ", "")')
        conn.commit()

        with pyodbc.connect(connection_str) as cnxn:
            cursor = cnxn.cursor()
            cnxn.autocommit = False
            cursor.fast_executemany = True
            cursor.execute("""IF OBJECT_ID('cont_total', 'U') IS NOT NULL DROP TABLE cont_total
            CREATE TABLE cont_total (id VARCHAR(50), A728_Act_priem VARCHAR(50), A402_dt_priem DATE, 
            A876_Act_zatar VARCHAR(50), A402_dt_zatar DATE,  A890_weightNet INT,  A1284_lot_no VARCHAR(50), 
            A752_Act_out VARCHAR(50), A402_dt_vivoz DATE, A746_book_no VARCHAR(50), 
            A1_rwb1 VARCHAR(50), A58_CarNo1 VARCHAR(50), A890_weightNet1 VARCHAR(50),
            A1_rwb2 VARCHAR(50), A58_CarNo2 VARCHAR(50), A890_weightNet2 VARCHAR(50))""")
            cnxn.commit()

            _execute(values, cnxn, cursor,
                     """INSERT INTO cont_total 
                     (id, A728_Act_priem, A402_dt_priem,  A876_Act_zatar, A402_dt_zatar, A890_weightNet,
                     A1284_lot_no, A752_Act_out, A402_dt_vivoz, A746_book_no,
                     A1_rwb1, A58_CarNo1, A890_weightNet1, A1_rwb2, A58_CarNo2, A890_weightNet2)
                     VALUES (?, ?, ?, ?, ?, ?,  ?, ?, ?, ?,  ?, ?, ?, ?, ?, ? )
                     """)

            cursor.execute("""UPDATE cont_total SET [id]=REPLACE ([id], ' ', '')""")
            cnxn.commit()

    print(datetime.now().strftime("%I:%M:%S "), 'total - OK')
