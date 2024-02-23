import psycopg2
from psycopg2.sql import SQL, Identifier

def create_db(conn):
    with conn.cursor() as cur:
        cur.execute("""
        DROP TABLE IF EXISTS client_phone;
        DROP TABLE IF EXISTS client_info;
        """)

        cur.execute("""
                CREATE TABLE IF NOT EXISTS client_info(
                client_id SERIAL PRIMARY KEY,
                client_name VARCHAR(20) NOT NULL,
                client_surname VARCHAR(20) NOT NULL,
                client_email VARCHAR(60) NOT NULL UNIQUE);
                """)

        cur.execute("""
                CREATE TABLE IF NOT EXISTS client_phone(
                client_phone VARCHAR(60),
                client_id INTEGER REFERENCES client_info(client_id));
                """)


def add_client(conn, client_name, client_surname, client_email, phones=None):
    conn.execute("""
        INSERT INTO client_info(client_name, client_surname, client_email)
        VALUES(%s, %s, %s)
        RETURNING client_id, client_name, client_surname, client_email;
        """, (client_name, client_surname, client_email))
    return cur.fetchall()


def add_phone(conn, client_id, client_phone):
    conn.execute("""
			INSERT INTO client_phone (client_id, client_phone)
			VALUES (%s,%s)
			RETURNING client_id, client_phone;
			""", (client_id, client_phone))
    return cur.fetchall()


def change_client(conn, client_id, client_name=None, client_surname=None, client_email=None):
    arg_list = {'client_name': client_name, 'client_surname': client_surname, 'client_email': client_email}
    for key, arg in arg_list.items():
        if arg:
            conn.execute(SQL("UPDATE client_info SET {}=%s WHERE client_id=%s").format(Identifier(key)), (arg, client_id))
    conn.execute("""
            SELECT * FROM client_info
            WHERE client_id=%s
            """, client_id)
    return cur.fetchall()


def delete_phone(conn, client_id, phones=None):
    conn.execute("""
            DELETE FROM client_phone
            WHERE client_id=%s
            """, (client_id,))
    conn.execute("""
            SELECT * FROM client_phone
            """, (phones, client_id))
    return cur.fetchall()


def delete_client(conn, client_id, client_name=None, client_surname=None, client_email=None, client_phones=None):
    conn.execute("""
            DELETE FROM client_info
            WHERE client_id=%s
            """, (client_id,))
    conn.execute("""
            SELECT * FROM client_info
            """, (client_name, client_surname, client_email, client_id))
    return cur.fetchall()


def find_client(conn, client_id, client_name=None, client_surname=None, client_email=None, client_phone=None):
      arg_list = {'client_name': client_name, "client_surname": client_surname, 'client_email': client_email, 'client_phone': client_phone}
      for key, arg in arg_list.items():
        if arg:
            conn.execute(SQL("UPDATE client_info SET {}=%s WHERE client_id=%s").format(Identifier(key)), (arg, client_id))
      conn.execute("""
            SELECT client_name, client_surname, client_email, client_phone FROM client_info AS c
            LEFT JOIN client_phone AS p ON c.client_id = p.client_id
            WHERE c.client_id=%s
            """, client_id)
      return cur.fetchall()


conn = psycopg2.connect(database='test_db', user='postgres', password='postgres')

create_db(conn)
with conn.cursor() as cur:
    print (add_client(cur, 'Иван','Петров', 'Petrov@mail.ru'))
    print (add_client(cur, 'Петр', 'Иванов', 'Ivaniv@mail.com'))
    print (add_client(cur, 'Семен', 'Сидоров', 'Sidorov@mail.com'))
    print (add_phone(cur, '1', ['89124657324', '89123674563']))
    print (add_phone(cur, '2', ['89063445673']))
    print (add_phone(cur, '3', ['89041223984', '89045674382']))
    print(change_client(cur, '1', client_name='Петр', client_email='Ivanov@mail.ru'))
    print (delete_phone(cur, '2'))
    print (delete_client(cur, '2'))
    print (find_client(cur,'1',))


conn.close()