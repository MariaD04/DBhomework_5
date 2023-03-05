import psycopg2

def create_db(conn):
    cur.execute("""
    CREATE TABLE IF NOT EXISTS Client(
        client_id SERIAL PRIMARY KEY,
        name VARCHAR(60) NOT NULL,
        surname VARCHAR(60) NOT NULL,
        email VARCHAR(60) NOT NULL UNIQUE);
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS Phone(
        number VARCHAR(60),
        client_id INTEGER NOT NULL REFERENCES Client(client_id));
    """)

def insert_client(conn, name, surname, email, number=None):
    cur.execute("""
    INSERT INTO Client(name, surname, email)
    VALUES(%s, %s, %s)
    RETURNING client_id, name, surname, email;
    """,(name, surname, email))
    return(conn.fetchall())

def insert_phone(conn, number, client_id):
    cur.execute("""
    INSERT INTO Phone(number, client_id)
    VALUES(%s, %s)
    RETURNING number, client_id;
    """,(number, client_id))
    return(conn.fetchall())

def update_data(conn, client_id, **kwargs_):
    fields_client = ('name', 'surname', 'email')
    fields_phone =('number')
    dict_client = {}
    dict_phone = {}
    for key, value in kwargs_.items():
        if key in fields_client:
            dict_client[key] = value
        elif key in fields_phone:
            dict_phone[key] = value
    
    data_client = tuple(zip(dict_client.keys(), dict_client.values()))
    list_client = [f"{x[0]} = '{x[1]}'" for x in data_client]
    fields_values_1 = ' , '.join(list_client)

    data_phone = tuple(zip(dict_phone.keys(), dict_phone.values()))
    list_phone = [f"{x[0]} = '{x[1]}'" for x in data_phone]
    fields_values_2 = ' , '.join(list_phone)
    
    if len(list_client) > 0:
        cur.execute(f"""
        UPDATE Client SET {fields_values_1}
        WHERE client_id = %s
        RETURNING client_id, name, surname, email;
        """, (client_id))

    if len(list_phone) > 0:
        cur.execute(f"""
        UPDATE Phone SET {fields_values_2}
        WHERE client_id = %s 
        RETURNING client_id, number;
        """, (client_id))

def delete_phone(conn, client_id, number):
    cur.execute("""
    DELETE FROM Phone WHERE client_id = %s AND number = %s;
    """, (client_id, number))

def delete_client(conn, client_id):
    cur.execute("""
    DELETE FROM Client WHERE client_id = %s;
    """, (client_id))

def find_client(**kwargs):
    fields = ('name', 'surname', 'email', 'number')

    for key, value in kwargs.items():
        if key not in fields:
            return 'fields not found'
    
    data = tuple(zip(kwargs.keys(), kwargs.values()))
    data_list = [f"{x[0]} = '{x[1]}'" for x in data]
    fields_values = ' and '.join(data_list)
    
    cur.execute(f"""
    SELECT name, surname, email, number FROM Client c
    JOIN Phone p USING(client_id)
    WHERE {fields_values} 
    """)
    return cur.fetchall()

with psycopg2.connect(database = 'db', user = 'postgres', password = 'postgresdb') as conn:
    with conn.cursor() as cur:
        '''
        cur.execute("""
            DROP TABLE Phone;
            DROP TABLE Client;
            """)
        '''
        if __name__ == '__main__':
            create_db(cur)

            print('Результат после добавления данных')

            print(insert_client(cur, 'Ivan', 'Ivanov', 'ivanovivan@mail.ru'))
            print(insert_client(cur, 'Sasha', 'Petrov', 'sashapetrov@mail.ru'))
            print(insert_phone(cur, '89276352415', '1'))
            print(insert_phone(cur, '89275632417', '2'))

            kwargs_ = {'name':'Andrey', 'email':'ivanovandrey@mail.ru', 'number':'89277594735'}
            update_data(cur, '1', **kwargs_)

            print()
            print('Результат после поиска данных')

            kwargs = {'name':'Sasha', 'surname':'Petrov'}
            print(find_client(**kwargs))

            kwargs = {'name':'Andrey', 'surname':'Ivanov'}
            print(find_client(**kwargs))

            delete_phone(cur, '1', '89277594735')
            delete_client(cur, '1')
  
conn.close()
