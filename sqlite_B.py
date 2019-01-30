import sqlite3


def execute_many(values, cn, cur, command):
    for i in range(int(len(values) / 1000) + 1):
        cur.executemany(command, values[i * 1000:(i + 1) * 1000])
        cn.commit()


def create_cursor(cn, drop, create):
    cur = cn.cursor()
    cur.execute('DROP TABLE IF EXISTS %s' % drop)
    cur.execute(create)
    cn.commit()
    return cur


def putosqlite_dic(values, connection_str, dic):
    if len(values) == 0:
        return
    with sqlite3.connect(connection_str) as conn:
        cursor = create_cursor(conn, dic, f'CREATE TABLE {dic} (id INTEGER, name1 TEXT, name2 TEXT)')

        execute_many(values, conn, cursor, f'INSERT INTO {dic} ( id, name1, name2 ) VALUES (?, ?, ? )')
