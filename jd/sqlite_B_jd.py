import sqlite3

from sqlite_B import create_cursor, execute_many


def putosqlite_B1(values, connection_str):
    if len(values) == 0:
        return
    with sqlite3.connect(connection_str) as conn:
        cursor = create_cursor(conn, 'rwbills',
            """CREATE TABLE rwbills (A1_RailWayBillNo TEXT, A473_Route INTEGER, A9_Date TEXT, A2_Sender INTEGER, 
            A4_Recipient INTEGER, A72_Country INTEGER, A10_Cargo INTEGER, A6_Qty INTEGER, A7_CargoNet NUMERIC, 
            A17_QtyReceived INTEGER, A54_CargoNetReceived NUMERIC, A557_Contract INTEGER, A558_Customer INTEGER, 
            A457_CargoName TEXT, A410_Recipient INTEGER, A108_Station INTEGER)""")

        execute_many(values, conn, cursor,
            """INSERT INTO rwbills (A1_RailWayBillNo, A473_Route, A9_Date, A2_Sender, A4_Recipient,
            A72_Country, A10_Cargo, A6_Qty, A7_CargoNet, A17_QtyReceived, A54_CargoNetReceived,
            A557_Contract, A558_Customer, A457_CargoName, A410_Recipient, A108_Station)
            VALUES (?, ?, ?, ?, ?,  ?, ?, ?, ?, ?, ?,   ?, ?, ?, ?, ?)""")


def putosqlite_B2(values, connection_str):
    if len(values) == 0:
        return
    with sqlite3.connect(connection_str) as conn:
        cursor = create_cursor(conn, 'rwbill_rows',
            """CREATE TABLE rwbill_rows (A1_RailWayBillNo TEXT, A18_ActNo TEXT, A58_CarNo TEXT, 
            A30_DateFinish TEXT, A31_TimeFinish TEXT, 
            A27_SectionNo INTEGER, A63_CargoNetDoc NUMERIC, A62_CargoNetFact NUMERIC, 
            A183_CarOs TEXT, A184_NetTotal NUMERIC, A5_Contract INTEGER, A144_GTD TEXT, 
            A8_RWB_Dop TEXT, A102_CertNo TEXT,
            A474_No TEXT, A475_NoFact TEXT, A481_Condition INTEGER, A482_State INTEGER, A483_SectionNo TEXT, 
            A601_Shipped NUMERIC, 
            A603_Spisano NUMERIC, A624_Carrying NUMERIC, A185_MCT_No TEXT, A563_Weight NUMERIC, A628_Custom TEXT, 
            A604_DeclWeight NUMERIC, A260_Decl TEXT, 
            A519_ContractNew INTEGER, A4_Owner INTEGER, A190_WeightInContainer NUMERIC)""")

        execute_many(values, conn, cursor,
            """INSERT INTO rwbill_rows ( A1_RailWayBillNo, A18_ActNo, A58_CarNo, A30_DateFinish, A31_TimeFinish, 
            A27_SectionNo, A63_CargoNetDoc, A62_CargoNetFact, 
            A183_CarOs, A184_NetTotal, A5_Contract, A144_GTD, A8_RWB_Dop, A102_CertNo,
            A474_No, A475_NoFact, A481_Condition, A482_State, A483_SectionNo, A601_Shipped, 
            A603_Spisano, A624_Carrying, A185_MCT_No, A563_Weight, A628_Custom, A604_DeclWeight, A260_Decl, 
            A519_ContractNew, A4_Owner, A190_WeightInContainer ) 
            VALUES (?, ?, ?, ?, ?,  ?, ?, ?,   ?, ?, ?, ?, ?, ?,
            ?, ?, ?, ?, ?, ?,  ?, ?, ?, ?, ?, ?, ?,  ?, ?, ? )""")


def putosqlite_B59(values, connection_str):
    if len(values) == 0:
        return
    with sqlite3.connect(connection_str) as conn:
        cursor = create_cursor(conn, 'rwtimes',
            """CREATE TABLE rwtimes (
            A1_RailWayBillNo TEXT, A58_CarNo TEXT, A367_DRaskred TEXT, A122_TRaskred TEXT, 
            A452_DVruchPam TEXT, A120_TVruchPam TEXT, A261_DFactGet TEXT, A31_TFactGet TEXT,
            A37_DCheck TEXT, A271_TCheck TEXT, A402_DPereDekl TEXT, A29_TPereDekl TEXT,
            A321_DUnload TEXT, A642_TUnload TEXT, A21_DPostCheck TEXT, A643_TPostCheck TEXT,
            A441_DOtstavka TEXT, A644_TOtstavka TEXT, A324_DIzOtsavka TEXT, A645_TIzOtsavka TEXT,

            A278_DPA TEXT, A646_TPA TEXT, A121_DNoticeOfUnload TEXT, A647_TNoticeOfUnload TEXT,
            A358_DJDNBack TEXT, A648_TJDNBack TEXT, A323_DFactBack TEXT, A649_TFactBack TEXT,
            A650_DForwarding TEXT, A651_TForwarding TEXT, 
            A20_NoDosil TEXT, A473_NoRoute TEXT, A561_NoPamiatka TEXT,
            A562_NoUvedom TEXT, A107_NoJDNBack TEXT,

            A652_DPamiatkaZakr TEXT, A653_TPamiatkaZakr TEXT, A656_DPeresilPoKvit TEXT, A657_TPeresilPoKvit TEXT,
            A43_NoKvit TEXT, A389_NoNaklPereOsvid TEXT, A268_DPereosvid TEXT, A269_TPereosvid TEXT, 
            A9_DOtprav TEXT, A28_DPredvSpis TEXT, A46_TPredvSpis TEXT, A512_NoExport TEXT,
            A52_DPA_MKR TEXT, A226_TPA_MKR TEXT)""")

        execute_many(values, conn, cursor,
            """INSERT INTO rwtimes (A1_RailWayBillNo, A58_CarNo, A367_DRaskred, A122_TRaskred, 
            A452_DVruchPam, A120_TVruchPam, A261_DFactGet, A31_TFactGet, 
            A37_DCheck, A271_TCheck, A402_DPereDekl, A29_TPereDekl,
            A321_DUnload, A642_TUnload, A21_DPostCheck, A643_TPostCheck,
            A441_DOtstavka, A644_TOtstavka, A324_DIzOtsavka, A645_TIzOtsavka,

            A278_DPA, A646_TPA, A121_DNoticeOfUnload, A647_TNoticeOfUnload,
            A358_DJDNBack, A648_TJDNBack, A323_DFactBack, A649_TFactBack, A650_DForwarding, A651_TForwarding,  
            A20_NoDosil, A473_NoRoute, A561_NoPamiatka, A562_NoUvedom, A107_NoJDNBack,             

            A652_DPamiatkaZakr, A653_TPamiatkaZakr, A656_DPeresilPoKvit, A657_TPeresilPoKvit,           
            A43_NoKvit, A389_NoNaklPereOsvid, A268_DPereosvid, A269_TPereosvid, 
            A9_DOtprav, A28_DPredvSpis, A46_TPredvSpis, A512_NoExport, A52_DPA_MKR, A226_TPA_MKR )  
            VALUES (?, ?, ?, ?,  ?, ?, ?, ?, ?, ?, ?, ?,  ?, ?, ?, ?, ?, ?, ?, ?,
            ?, ?, ?, ?,   ?, ?, ?, ?, ?, ?,   ?, ?, ?, ?, ?,   ?, ?, ?, ?, ?, ?, ?, ?, 
            ?, ?, ?, ?, ?, ?)""")


def putosqlite_B11(values, connection_str):
    if len(values) == 0:
        return
    with sqlite3.connect(connection_str) as conn:
        cursor = create_cursor(conn, 'rwbill_returns',
            """CREATE TABLE rwbill_returns (
            A1_rwb INTEGER, ND_rwb TEXT, A9_Date TEXT, A2_Sender INTEGER, A224_recipient INTEGER, A6_Qty INTEGER, 
            A474_route TEXT, A553_info TEXT, A554_info TEXT, 
            A46_time TEXT, A108_station INTEGER, A107_recipient_rwb TEXT, 
            A85_address TEXT, A560_station_from INTEGER, A462_state INTEGER, A66_No INTEGER)""")

        execute_many(values, conn, cursor,
            """INSERT INTO rwbill_returns (A1_rwb, ND_rwb, A9_date, A2_sender, A224_recipient, A6_Qty, 
            A474_route, A553_info, A554_info, A46_time, A108_station, A107_recipient_rwb, 
            A85_address, A560_station_from, A462_state, A66_No )  
            VALUES (?, ?, ?, ?, ?, ?,  ?, ?, ?, ?, ?, ?,  ?, ?, ?, ?)""")


def putosqlite_B11S1(values, connection_str):
    if len(values) == 0:
        return
    with sqlite3.connect(connection_str) as conn:
        cursor = create_cursor(conn, 'rwbill_return_rows',
            """CREATE TABLE rwbill_return_rows (A1_rwb INTEGER, A58_CarNo TEXT, 
            A474_orderNo INTEGER, A473_route TEXT, A65_doc TEXT, A563_weght TEXT)""")

        execute_many(values, conn, cursor,
            'INSERT INTO rwbill_return_rows ( A1_rwb, A58_CarNo, A474_orderNo, A473_route, A65_doc, A563_weght ) '
            ' VALUES (?, ?, ?, ?, ?, ? )')


def putosqlite_B49S2(values, connection_str):
    if len(values) == 0:
        return
    with sqlite3.connect(connection_str) as conn:
        cursor = create_cursor(conn, 'route_notice',
            """CREATE TABLE route_notice (A474_route TEXT, A561_memo TEXT, A562_notice TEXT, A1_rwb TEXT, A58_CarNo TEXT)""")

        execute_many(values, conn, cursor,
            'INSERT INTO route_notice ( A474_route, A561_memo, A562_notice, A1_rwb, A58_CarNo ) '
            ' VALUES (?, ?, ?, ?, ? )')



def putosqlite_B49S1(values, connection_str):
    if len(values) == 0:
        return
    with sqlite3.connect(connection_str) as conn:
        cursor = create_cursor(conn, 'route_notice_time',
            """CREATE TABLE route_notice_time (A474_route TEXT, A561_memo TEXT, A562_notice TEXT, A556_path_no TEXT, 
            A28_date_in TEXT, A29_time_in TEXT, A30_date_end TEXT, A31_time_end TEXT)""")

        execute_many(values, conn, cursor,
            'INSERT INTO route_notice_time ( A474_route, A561_memo, A562_notice, A556_path_no, '
            'A28_date_in, A29_time_in, A30_date_end, A31_time_end ) '
            ' VALUES (?, ?, ?, ?, ?, ?, ?, ? )')


# def putosqlite_B12A144(values, connection_str):
#     if len(values) == 0:
#         return
#     with sqlite3.connect(connection_str) as conn:
#         cursor = create_cursor(conn, 'gtd',
#             'CREATE TABLE gtd (A144_gtd TEXT, A1_rwb TEXT, A58_CarNo TEXT)')
#
#         execute_many(values, conn, cursor,
#             'INSERT INTO gtd (A144_gtd, A1_rwb, A58_CarNo) VALUES (?, ?, ? )')
