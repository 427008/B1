import pyodbc


def putomssql_dic(values, connection_str, dic):
    if len(values) == 0:
        return
    with pyodbc.connect(connection_str) as cnxn:
        cursor = cnxn.cursor()
        cnxn.autocommit = False
        cursor.fast_executemany = True
        cursor.execute(f"""IF OBJECT_ID('{dic}', 'U') IS NOT NULL DROP TABLE {dic}
        CREATE TABLE {dic} (id INT, name1 VARCHAR(255), name2 VARCHAR(255))""")
        cnxn.commit()

        for i in range(int(len(values) / 1000) + 1):
            cursor.executemany(f'INSERT INTO {dic} (id, name1, name2) VALUES ( ?, ?, ? )',
            values[i * 1000:(i + 1) * 1000])
            cnxn.commit()