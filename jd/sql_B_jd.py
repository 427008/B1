import pyodbc


def putomssql_B1(values, connection_str):
    if len(values) == 0:
        return
    with pyodbc.connect(connection_str) as cnxn:
        cursor = cnxn.cursor()
        cnxn.autocommit = False
        cursor.fast_executemany = True
        cursor.execute("""IF OBJECT_ID('rwbill', 'U') IS NOT NULL DROP TABLE rwbill
        CREATE TABLE rwbill (A1_RailWayBillNo VARCHAR(50), A473_Route INT, A9_Date DATE, A2_Sender INT, A4_Recipient INT,
            A72_Country INT, A10_Cargo INT, A6_Qty INT, A7_CargoNet MONEY, A17_QtyReceived INT, A54_CargoNetReceived MONEY,
            A557_Contract INT, A558_Customer INT, A457_CargoName VARCHAR(255), A410_Recipient INT, A108_Station INT)""")
        cnxn.commit()

        for i in range(int(len(values) / 1000) + 1):
            cursor.executemany("""
            INSERT INTO rwbill (A1_RailWayBillNo, A473_Route, A9_Date, A2_Sender, A4_Recipient,
            A72_Country, A10_Cargo, A6_Qty, A7_CargoNet, A17_QtyReceived, A54_CargoNetReceived,
            A557_Contract, A558_Customer, A457_CargoName, A410_Recipient, A108_Station)
            VALUES ( ?, ?, ?, ?, ?,  ?, ?, ?, ?, ?, ?,   ?, ?, ?, ?, ? )
            """,
            values[i * 1000:(i + 1) * 1000])
            cnxn.commit()


def putomssql_B2(values, connection_str):
    if len(values) == 0:
        return
    with pyodbc.connect(connection_str) as cnxn:
        cursor = cnxn.cursor()
        cnxn.autocommit = False
        cursor.fast_executemany = True
        cursor.execute("""IF OBJECT_ID('rwbill_row', 'U') IS NOT NULL DROP TABLE rwbill_row
        CREATE TABLE rwbill_row (
            A1_RailWayBillNo VARCHAR(50), A18_ActNo VARCHAR(50), A58_CarNo VARCHAR(50), 
            A30_DateFinish DATE, A31_TimeFinish CHAR(5), 
            A27_SectionNo INT, A63_CargoNetDoc MONEY, A62_CargoNetFact MONEY, 
            A183_CarOs VARCHAR(50), A184_NetTotal MONEY, A5_Contract INT, A144_GTD VARCHAR(50), 
            A8_RWB_Dop VARCHAR(50), A102_CertNo VARCHAR(50),
            A474_No CHAR(5), A475_NoFact CHAR(5), A481_Condition INT, A482_State INT, A483_SectionNo VARCHAR(50), 
            A601_Shipped MONEY, 
            A603_Spisano MONEY, A624_Carrying MONEY, A185_MCT_No CHAR(6), A563_Weight MONEY, A628_Custom VARCHAR(50), 
            A604_DeclWeight MONEY, A260_Decl VARCHAR(50), 
            A519_ContractNew INT, A4_Owner INT, A190_WeightInContainer MONEY)""")
        cnxn.commit()

        for i in range(int(len(values) / 1000) + 1):
            cursor.executemany("""
            INSERT INTO rwbill_row (A1_RailWayBillNo, A18_ActNo, A58_CarNo, A30_DateFinish, A31_TimeFinish, 
                A27_SectionNo, A63_CargoNetDoc, A62_CargoNetFact, 
                A183_CarOs, A184_NetTotal, A5_Contract, A144_GTD, A8_RWB_Dop, A102_CertNo,
                A474_No, A475_NoFact, A481_Condition, A482_State, A483_SectionNo, A601_Shipped, 
                A603_Spisano, A624_Carrying, A185_MCT_No, A563_Weight, A628_Custom, A604_DeclWeight, A260_Decl, 
                A519_ContractNew, A4_Owner, A190_WeightInContainer) 
            VALUES ( ?, ?, ?, ?, ?,  ?, ?, ?,   ?, ?, ?, ?, ?, ?,
            ?, ?, ?, ?, ?, ?,  ?, ?, ?, ?, ?, ?, ?,  ?, ?, ? )
            """,
            values[i * 1000:(i + 1) * 1000])
            cnxn.commit()


def putomssql_B59(values, connection_str):
    if len(values) == 0:
        return
    with pyodbc.connect(connection_str) as cnxn:
        cursor = cnxn.cursor()
        cnxn.autocommit = False
        cursor.fast_executemany = True
        cursor.execute("""IF OBJECT_ID('rwtime', 'U') IS NOT NULL DROP TABLE rwtime
        CREATE TABLE rwtime (
            A1_RailWayBillNo VARCHAR(50), A58_CarNo VARCHAR(50), A367_DRaskred DATE, A122_TRaskred CHAR(5), 
            A452_DVruchPam DATE, A120_TVruchPam CHAR(5), A261_DFactGet DATE, A31_TFactGet CHAR(5),
            A37_DCheck DATE, A271_TCheck CHAR(5), A402_DPereDekl DATE, A29_TPereDekl CHAR(5),
            A321_DUnload DATE, A642_TUnload CHAR(5), A21_DPostCheck DATE, A643_TPostCheck CHAR(5),
            A441_DOtstavka DATE, A644_TOtstavka CHAR(5), A324_DIzOtsavka DATE, A645_TIzOtsavka CHAR(5),

            A278_DPA DATE, A646_TPA CHAR(5), A121_DNoticeOfUnload DATE, A647_TNoticeOfUnload CHAR(5),
            A358_DJDNBack DATE, A648_TJDNBack CHAR(5), A323_DFactBack DATE, A649_TFactBack CHAR(5),
            A650_DForwarding DATE, A651_TForwarding CHAR(5), 
            A20_NoDosil VARCHAR(50), A473_NoRoute VARCHAR(50), A561_NoPamiatka VARCHAR(50),
            A562_NoUvedom VARCHAR(50), A107_NoJDNBack VARCHAR(50),

            A652_DPamiatkaZakr DATE, A653_TPamiatkaZakr CHAR(5), A656_DPeresilPoKvit DATE, A657_TPeresilPoKvit CHAR(5),
            A43_NoKvit VARCHAR(50), A389_NoNaklPereOsvid VARCHAR(50), A268_DPereosvid DATE, A269_TPereosvid CHAR(5), 
            A9_DOtprav DATE, A28_DPredvSpis DATE, A46_TPredvSpis CHAR(5), A512_NoExport VARCHAR(50),
            A52_DPA_MKR DATE, A226_TPA_MKR CHAR(5))""")
        cnxn.commit()

        for i in range(int(len(values) / 1000) + 1):
            cursor.executemany("""
            INSERT INTO rwtime (A1_RailWayBillNo, A58_CarNo, A367_DRaskred, A122_TRaskred, 
            A452_DVruchPam, A120_TVruchPam, A261_DFactGet, A31_TFactGet, 
            A37_DCheck, A271_TCheck, A402_DPereDekl, A29_TPereDekl,
            A321_DUnload, A642_TUnload, A21_DPostCheck, A643_TPostCheck,
            A441_DOtstavka, A644_TOtstavka, A324_DIzOtsavka, A645_TIzOtsavka,

            A278_DPA, A646_TPA, A121_DNoticeOfUnload, A647_TNoticeOfUnload,
            A358_DJDNBack, A648_TJDNBack, A323_DFactBack, A649_TFactBack, A650_DForwarding, A651_TForwarding,  
            A20_NoDosil, A473_NoRoute, A561_NoPamiatka, A562_NoUvedom, A107_NoJDNBack,             

            A652_DPamiatkaZakr, A653_TPamiatkaZakr, A656_DPeresilPoKvit, A657_TPeresilPoKvit,           
            A43_NoKvit, A389_NoNaklPereOsvid, A268_DPereosvid, A269_TPereosvid, 
            A9_DOtprav, A28_DPredvSpis, A46_TPredvSpis, A512_NoExport, A52_DPA_MKR, A226_TPA_MKR)  
            VALUES (?, ?, ?, ?,  ?, ?, ?, ?, ?, ?, ?, ?,  ?, ?, ?, ?, ?, ?, ?, ?,
            ?, ?, ?, ?,   ?, ?, ?, ?, ?, ?,   ?, ?, ?, ?, ?,   ?, ?, ?, ?, ?, ?, ?, ?, 
             ?, ?, ?, ?, ?, ?)""",
            values[i * 1000:(i + 1) * 1000])
            cnxn.commit()


def putomssql_B11(values, connection_str):
    if len(values) == 0:
        return
    with pyodbc.connect(connection_str) as cnxn:
        cursor = cnxn.cursor()
        cnxn.autocommit = False
        cursor.fast_executemany = True
        cursor.execute("""IF OBJECT_ID('rwbill_return', 'U') IS NOT NULL DROP TABLE rwbill_return
        CREATE TABLE rwbill_return (
            A1_rwb INT, ND_rwb VARCHAR(255), A9_Date DATE, A2_Sender INT, A224_recipient INT, A6_Qty INT, 
            A474_route VARCHAR(50), A553_info VARCHAR(255), A554_info VARCHAR(255), 
            A46_time CHAR(6), A108_station INT, A107_recipient_rwb VARCHAR(50), 
            A85_address VARCHAR(255), A560_station_from INT, A462_state INT, A66_No INT)""")
        cnxn.commit()

        for i in range(int(len(values) / 1000) + 1):
            cursor.executemany('INSERT INTO rwbill_return (A1_rwb, ND_rwb, A9_date, A2_sender, A224_recipient, '
                               'A6_Qty, A474_route, A553_info, A554_info, A46_time, A108_station, A107_recipient_rwb, ' 
                               'A85_address, A560_station_from, A462_state, A66_No) '  
                               'VALUES ( ?, ?, ?, ?, ?, ?,  ?, ?, ?, ?, ?, ?,  ?, ?, ?, ? )',
            values[i * 1000:(i + 1) * 1000])
            cnxn.commit()


def putomssql_B11S1(values, connection_str):
    if len(values) == 0:
        return
    with pyodbc.connect(connection_str) as cnxn:
        cursor = cnxn.cursor()
        cnxn.autocommit = False
        cursor.fast_executemany = True
        cursor.execute("""IF OBJECT_ID('rwbill_return_row', 'U') IS NOT NULL DROP TABLE rwbill_return_row
        CREATE TABLE rwbill_return_row (
            A1_rwb INT, A58_CarNo VARCHAR(50), A474_orderNo INT, 
            A473_route VARCHAR(50), A65_doc VARCHAR(50), A563_weght VARCHAR(50))""")
        cnxn.commit()

        for i in range(int(len(values) / 1000) + 1):
            cursor.executemany(' INSERT INTO rwbill_return_row (A1_rwb, A58_CarNo, A474_orderNo, '
                               'A473_route, A65_doc, A563_weght ) VALUES (?, ?, ?, ?, ?, ? )',
            values[i * 1000:(i + 1) * 1000])
            cnxn.commit()


def putomssql_B49S2(values, connection_str):
    if len(values) == 0:
        return
    with pyodbc.connect(connection_str) as cnxn:
        cursor = cnxn.cursor()
        cnxn.autocommit = False
        cursor.fast_executemany = True
        cursor.execute("""IF OBJECT_ID('route_notice', 'U') IS NOT NULL DROP TABLE route_notice
        CREATE TABLE route_notice (A474_route VARCHAR(50), A561_memo VARCHAR(50), A562_notice VARCHAR(50), 
            A1_rwb VARCHAR(50), A58_CarNo VARCHAR(50))""")
        cnxn.commit()

        for i in range(int(len(values) / 1000) + 1):
            cursor.executemany(' INSERT INTO route_notice (A474_route, A561_memo, A562_notice, A1_rwb, A58_CarNo ) '
                               'VALUES (?, ?, ?, ?, ? )',
            values[i * 1000:(i + 1) * 1000])
            cnxn.commit()


def putomssql_B49S1(values, connection_str):
    if len(values) == 0:
        return
    with pyodbc.connect(connection_str) as cnxn:
        cursor = cnxn.cursor()
        cnxn.autocommit = False
        cursor.fast_executemany = True
        cursor.execute("""IF OBJECT_ID('route_notice_time', 'U') IS NOT NULL DROP TABLE route_notice_time
        CREATE TABLE route_notice_time (A474_route VARCHAR(50), A561_memo VARCHAR(50), A562_notice VARCHAR(50), 
            A556_path_no VARCHAR(50), A28_date_in DATE, A29_time_in CHAR(5), A30_date_end DATE, A31_time_end CHAR(5))""")
        cnxn.commit()

        for i in range(int(len(values) / 1000) + 1):
            cursor.executemany('INSERT INTO route_notice_time (A474_route, A561_memo, A562_notice, A556_path_no, '
            'A28_date_in, A29_time_in, A30_date_end, A31_time_end ) VALUES (?, ?, ?, ?, ?, ?, ?, ? )',
            values[i * 1000:(i + 1) * 1000])
            cnxn.commit()


# def putomssql_B12A144(values, connection_str):
#     if len(values) == 0:
#         return
#     with pyodbc.connect(connection_str) as cnxn:
#         cursor = cnxn.cursor()
#         cnxn.autocommit = False
#         cursor.fast_executemany = True
#         cursor.execute("""IF OBJECT_ID('gtd', 'U') IS NOT NULL DROP TABLE B12A144_gtd
#         CREATE TABLE gtd (A144_gtd VARCHAR(255), A1_rwb VARCHAR(50), A58_CarNo VARCHAR(50))""")
#         cnxn.commit()
#
#         for i in range(int(len(values) / 1000) + 1):
#             cursor.executemany('INSERT INTO gtd (A144_gtd, A1_rwb, A58_CarNo) VALUES (?, ?, ? )',
#             values[i * 1000:(i + 1) * 1000])
#             cnxn.commit()
