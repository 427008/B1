import sqlite3

from sqlite_B import create_cursor, execute_many


def putosqlite_BE5(values, connection_str):
    if len(values) == 0:
        return
    with sqlite3.connect(connection_str) as conn:
        cursor = create_cursor(conn, 'cont_priem',
            """CREATE TABLE cont_priem (id TEXT, A728_Act_priem TEXT, A402_dt TEXT, A673_tm TEXT, A841_pc TEXT,
            A842_nc TEXT, A866_fio_cdal INTEGER, A867_fio_prinial INTEGER, A740_code_def TEXT, A864_info TEXT,
            A296_oper TEXT, A830_zajavka TEXT, A0 INTEGER)""")

        execute_many(values, conn, cursor,
            'INSERT INTO cont_priem ( id, A728_Act_priem, A402_dt, A673_tm, A841_pc, A842_nc, '
            'A866_fio_cdal, A867_fio_prinial, A740_code_def, A864_info, A296_oper, A830_zajavka, A0)'
            'VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)')


def putosqlite_BE7(values, connection_str, recreate=True):
    if len(values) == 0:
        return
    with sqlite3.connect(connection_str) as conn:
        if recreate:
            cursor = create_cursor(conn, 'cont_zatar',
                """CREATE TABLE cont_zatar (id TEXT, A876_Act_zatar TEXT, A402_dt TEXT, A673_tm TEXT, 
                A841_pc TEXT, A842_nc TEXT, A893_SEZ_dt TEXT, A296_oper TEXT,  A830_zajavka TEXT, 
                A741_state INTEGER, A829_Act_plomb TEXT, A890_weightNet INTEGER, A771_weightBrtFact INTEGER,  
                A849_Krepl INTEGER, A889_weightPorojn INTEGER, 
                A953_Act_to_KP TEXT, A956_Act_to_Stek TEXT, A867_fio_prinial INTEGER,  
                A828_tamoj TEXT,  A45_Surv TEXT, A833_Contract INTEGER,  A832_Owner INTEGER, A834_predsavitel INTEGER, 
                A10_gross INTEGER, A863_WesKreplenia INTEGER, A1284_lot_no TEXT, 
                AX1 INTEGER, AX2 INTEGER, AX3 INTEGER, AX4 INTEGER, AX5 INTEGER, AX6 INTEGER)""")
        else:
            cursor = conn.cursor()

        execute_many(values, conn, cursor,
            'INSERT INTO cont_zatar ( id, A876_Act_zatar, A402_dt, A673_tm, A841_pc, A842_nc, A893_SEZ_dt, A296_oper, '
            'A830_zajavka, A741_state, A829_Act_plomb, A890_weightNet, A771_weightBrtFact, A849_Krepl, '
            'A889_weightPorojn, A953_Act_to_KP, A956_Act_to_Stek, A867_fio_prinial, A828_tamoj, A45_Surv, '
            'A833_Contract, A832_Owner, A834_predsavitel, A10_gross, A863_WesKreplenia, A1284_lot_no, '
            'AX1, AX2, AX3, AX4, AX5, AX6 ) '
            'VALUES (?, ?, ?, ?, ?, ?,  ?, ?, ?,  ?, ?, ?, ?, ?, ?,  ?, ?, ?,  ?, ?, ?, ?, ?, ?, ?, ?,  ?, ?, ?, ?, ?, ? )')


def putosqlite_BE23(values, connection_str):
    if len(values) == 0:
        return
    with sqlite3.connect(connection_str) as conn:
        cursor = create_cursor(conn, 'cont_vivoz',
            """CREATE TABLE cont_vivoz (id TEXT, A752_Act_out TEXT, A402_dt TEXT, A673_tm TEXT, A841_pc TEXT,
            A842_nc TEXT, A866_fio_prinial INTEGER, A867_fio_cdal INTEGER, A740_code_def TEXT, 
            A296_oper TEXT, A746_book_no TEXT)""")

        execute_many(values, conn, cursor,
            'INSERT INTO cont_vivoz ( id, A752_Act_out, A402_dt, A673_tm, A841_pc, A842_nc, '
            'A866_fio_prinial, A867_fio_cdal, A740_code_def, A296_oper, A746_book_no) '
            'VALUES (?, ?, ?, ?, ?, ?,  ?, ?, ?, ?, ?)')


def putosqlite_BE7S1(values, connection_str, recreate=True):
    if len(values) == 0:
        return
    with sqlite3.connect(connection_str) as conn:
        if recreate:
            cursor = create_cursor(conn, 'cont_jd',
                """CREATE TABLE cont_jd (A876_Act TEXT, A1_rwb TEXT, A58_CarNo TEXT, A144_decl_no TEXT,
                A890_weightNet TEXT, AX1 INTEGER, A452_dt TEXT, A642_tm TEXT)""")
        else:
            cursor = conn.cursor()

        execute_many(values, conn, cursor,
            'INSERT INTO cont_jd ( A876_Act, A1_rwb, A58_CarNo, A144_decl_no, A890_weightNet, AX1, A452_dt, A642_tm) '
            'VALUES (?, ?, ?, ?, ?, ?, ?, ?)')


def putosqlite_BE1(values, connection_str):
    if len(values) == 0:
        return
    with sqlite3.connect(connection_str) as conn:
        cursor = create_cursor(conn, 'cont_zajav',
            """CREATE TABLE cont_zajav (A830_Zajav TEXT, A831_Zajav_p TEXT, A1001_dt TEXT, 
            A832_Owner INTEGER, A833_Contract INTEGER, 
            A367_dt_n TEXT, A673_tm_n TEXT, A452_dt_k TEXT, A642_tm_k TEXT, A296_oper TEXT, A738_state INTEGER, 
            A834_predsavitel INTEGER, A835_sviaz TEXT, A839_dover TEXT, A1284_lot_no TEXT)""")

        execute_many(values, conn, cursor,
            'INSERT INTO cont_zajav ( A830_Zajav, A831_Zajav_p, A1001_dt, A832_Owner, A833_Contract, '
            'A367_dt_n, A673_tm_n, A452_dt_k, A642_tm_k, A296_oper, A738_state, '
            'A834_predsavitel, A835_sviaz, A839_dover, A1284_lot_no) '
            'VALUES (?, ?, ?, ?, ?,  ?, ?, ?, ?, ?, ?,  ?, ?, ?, ? )')