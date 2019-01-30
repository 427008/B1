import pyodbc


def putomssql_BE5(values, connection_str):
    if len(values) == 0:
        return
    with pyodbc.connect(connection_str) as cnxn:
        cursor = cnxn.cursor()
        cnxn.autocommit = False
        cursor.fast_executemany = True
        cursor.execute("""IF OBJECT_ID('cont_priem', 'U') IS NOT NULL DROP TABLE cont_priem
        CREATE TABLE cont_priem (id VARCHAR(18), A728_Act_priem VARCHAR(50), A402_dt DATE, A673_tm CHAR(5), 
            A841_pc VARCHAR(9), A842_nc VARCHAR(9), A866_fio_cdal INT, A867_fio_prinial INT, A740_code_def VARCHAR(255), 
            A864_info VARCHAR(255), A296_oper VARCHAR(50), A830_zajavka VARCHAR(50), A0 INT)""")
        cnxn.commit()

        for i in range(int(len(values) / 1000) + 1):
            cursor.executemany('INSERT INTO cont_priem ( id, A728_Act_priem, A402_dt, A673_tm, A841_pc, A842_nc, '
                               'A866_fio_cdal, A867_fio_prinial, A740_code_def, A864_info, A296_oper, '
                               'A830_zajavka, A0) '
                               'VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)',
            values[i * 1000:(i + 1) * 1000])
            cnxn.commit()


def putomssql_BE7(values, connection_str, recreate=True):
    if len(values) == 0:
        return
    with pyodbc.connect(connection_str) as cnxn:
        cursor = cnxn.cursor()
        cnxn.autocommit = False
        cursor.fast_executemany = True
        if recreate:
            cursor.execute("""IF OBJECT_ID('cont_zatar', 'U') IS NOT NULL DROP TABLE cont_zatar
            CREATE TABLE cont_zatar (id VARCHAR(18), A876_Act_zatar VARCHAR(50), A402_dt DATE, A673_tm CHAR(5), 
                A841_pc VARCHAR(9), A842_nc VARCHAR(9), A893_SEZ_dt DATE, A296_oper VARCHAR(50), A830_zajavka VARCHAR(50), 
                A741_state INT, A829_Act_plomb VARCHAR(50), A890_weightNet INT, A771_weightBrtFact INT,  
                A849_Krepl INT, A889_weightPorojn INT, 
                A953_Act_to_KP VARCHAR(50), A956_Act_to_Stek VARCHAR(50), A867_fio_prinial INT,  
                A828_tamoj VARCHAR(50),  A45_Surv VARCHAR(50), A833_Contract INT,  A832_Owner INT, A834_predsavitel INT, 
                A10_gross INT, A863_WesKreplenia INT, A1284_lot_no VARCHAR(50),
                AX1 INT, AX2 INT, AX3 INT, AX4 INT, AX5 INT, AX6 INT)""")
            cnxn.commit()

        for i in range(int(len(values) / 1000) + 1):
            cursor.executemany('INSERT INTO cont_zatar ( id, A876_Act_zatar, A402_dt, A673_tm, A841_pc, A842_nc, '
                               'A893_SEZ_dt, A296_oper, A830_zajavka, '
                               'A741_state, A829_Act_plomb, A890_weightNet, A771_weightBrtFact, '
                               'A849_Krepl, A889_weightPorojn, '
                               'A953_Act_to_KP, A956_Act_to_Stek, A867_fio_prinial, '
                               'A828_tamoj, A45_Surv, A833_Contract, A832_Owner, A834_predsavitel, '
                               'A10_gross, A863_WesKreplenia, A1284_lot_no, AX1, AX2, AX3, AX4, AX5, AX6 ) '
                               'VALUES (?, ?, ?, ?, ?, ?,  ?, ?, ?,  ?, ?, ?, ?, ?, ?,  ?, ?, ?,  ?, ?, ?, ?, ?, '
                               '?, ?, ?,  ?, ?, ?, ?, ?, ? )',
            values[i * 1000:(i + 1) * 1000])
            cnxn.commit()


def putomssql_BE23(values, connection_str):
    if len(values) == 0:
        return
    with pyodbc.connect(connection_str) as cnxn:
        cursor = cnxn.cursor()
        cnxn.autocommit = False
        cursor.fast_executemany = True
        cursor.execute("""IF OBJECT_ID('cont_vivoz', 'U') IS NOT NULL DROP TABLE cont_vivoz
        CREATE TABLE cont_vivoz (id VARCHAR(18), A752_Act_out VARCHAR(50), A402_dt DATE, A673_tm CHAR(5), 
            A841_pc VARCHAR(9), A842_nc VARCHAR(9), A866_fio_prinial INT, A867_fio_cdal INT, A740_code_def VARCHAR(255), 
            A296_oper VARCHAR(50), A746_book_no VARCHAR(50))""")
        cnxn.commit()

        for i in range(int(len(values) / 1000) + 1):
            cursor.executemany('INSERT INTO cont_vivoz ( id, A752_Act_out, A402_dt, A673_tm, A841_pc, A842_nc, '
                               'A866_fio_prinial, A867_fio_cdal, A740_code_def, A296_oper, A746_book_no) '
                               'VALUES (?, ?, ?, ?, ?, ?,  ?, ?, ?, ?, ?)',
            values[i * 1000:(i + 1) * 1000])
            cnxn.commit()

def putomssql_BE7S1(values, connection_str, recreate=True):
    if len(values) == 0:
        return
    with pyodbc.connect(connection_str) as cnxn:
        cursor = cnxn.cursor()
        cnxn.autocommit = False
        cursor.fast_executemany = True
        if recreate:
            cursor.execute("""IF OBJECT_ID('cont_jd', 'U') IS NOT NULL DROP TABLE cont_jd
            CREATE TABLE cont_jd (A876_Act VARCHAR(50), A1_rwb VARCHAR(50), A58_CarNo VARCHAR(50), A144_decl_no VARCHAR(50),
                A890_weightNet VARCHAR(50), AX1 INT, A452_dt DATE, A642_tm CHAR(5))""")
            cnxn.commit()

        for i in range(int(len(values) / 1000) + 1):
            cursor.executemany('INSERT INTO cont_jd ( A876_Act, A1_rwb, A58_CarNo, A144_decl_no, '
                               'A890_weightNet, AX1, A452_dt, A642_tm) '
                               'VALUES (?, ?, ?, ?, ?, ?, ?, ?)',
            values[i * 1000:(i + 1) * 1000])
            cnxn.commit()


def putomssql_BE1(values, connection_str):
    if len(values) == 0:
        return
    with pyodbc.connect(connection_str) as cnxn:
        cursor = cnxn.cursor()
        cnxn.autocommit = False
        cursor.fast_executemany = True
        cursor.execute("""IF OBJECT_ID('cont_zajav', 'U') IS NOT NULL DROP TABLE cont_zajav
            CREATE TABLE cont_zajav (A830_Zajav VARCHAR(50), A831_Zajav_p VARCHAR(50), A1001_dt DATE, 
                A832_Owner INT, A833_Contract INT, 
                A367_dt_n DATE, A673_tm_n CHAR(5), A452_dt_k DATE, A642_tm_k CHAR(5), A296_oper VARCHAR(50), 
                A738_state INT, A834_predsavitel INT, 
                A835_sviaz VARCHAR(255), A839_dover VARCHAR(50), A1284_lot_no VARCHAR(50))""")
        cnxn.commit()

        for i in range(int(len(values) / 1000) + 1):
            cursor.executemany('INSERT INTO cont_zajav ( A830_Zajav, A831_Zajav_p, A1001_dt, A832_Owner, A833_Contract, '
                               'A367_dt_n, A673_tm_n, A452_dt_k, A642_tm_k, A296_oper, A738_state, '
                               'A834_predsavitel, A835_sviaz, A839_dover, A1284_lot_no) '
                               'VALUES (?, ?, ?, ?, ?,  ?, ?, ?, ?, ?, ?,  ?, ?, ?, ? )',
            values[i * 1000:(i + 1) * 1000])
            cnxn.commit()

