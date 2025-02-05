import psycopg2
from psycopg2 import sql

DB_NAME = "clients_data_base"
DB_USER = "postgres"
DB_PASSWORD = "*********"
DB_HOST = "localhost"
DB_PORT = "5432"

def connect_db():
    return psycopg2.connect(
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        host=DB_HOST,
        port=DB_PORT
    )

def create_db():
    conn = connect_db()
    cur = conn.cursor()
    
    cur.execute("""
    CREATE TABLE IF NOT EXISTS clients (
        id SERIAL PRIMARY KEY,
        first_name VARCHAR(50) NOT NULL,
        last_name VARCHAR(50) NOT NULL,
        email VARCHAR(100) UNIQUE
    )
    """)
    
    cur.execute("""
    CREATE TABLE IF NOT EXISTS phones (
        id SERIAL PRIMARY KEY,
        client_id INTEGER REFERENCES clients(id) ON DELETE CASCADE,
        phone VARCHAR(20)
    )
    """)
    
    conn.commit()
    cur.close()
    conn.close()

def add_client(first_name, last_name, email, phones=None):
    conn = connect_db()
    cur = conn.cursor()
    
    cur.execute(
        "INSERT INTO clients (first_name, last_name, email) VALUES (%s, %s, %s) RETURNING id",
        (first_name, last_name, email)
    )
    client_id = cur.fetchone()[0]
    
    if phones:
        for phone in phones:
            cur.execute(
                "INSERT INTO phones (client_id, phone) VALUES (%s, %s)",
                (client_id, phone)
            )
    
    conn.commit()
    cur.close()
    conn.close()
    return client_id

def add_phone(client_id, phone):
    conn = connect_db()
    cur = conn.cursor()
    
    cur.execute(
        "INSERT INTO phones (client_id, phone) VALUES (%s, %s)",
        (client_id, phone)
    )
    
    conn.commit()
    cur.close()
    conn.close()

def update_client(client_id, first_name=None, last_name=None, email=None):
    conn = connect_db()
    cur = conn.cursor()
    
    update_fields = []
    update_values = []
    
    if first_name:
        update_fields.append("first_name = %s")
        update_values.append(first_name)
    if last_name:
        update_fields.append("last_name = %s")
        update_values.append(last_name)
    if email:
        update_fields.append("email = %s")
        update_values.append(email)
    
    if update_fields:
        query = sql.SQL("UPDATE clients SET {} WHERE id = %s").format(
            sql.SQL(", ").join(map(sql.SQL, update_fields))
        )
        cur.execute(query, update_values + [client_id])
    
    conn.commit()
    cur.close()
    conn.close()

def delete_phone(client_id, phone):
    conn = connect_db()
    cur = conn.cursor()
    
    cur.execute(
        "DELETE FROM phones WHERE client_id = %s AND phone = %s",
        (client_id, phone)
    )
    
    conn.commit()
    cur.close()
    conn.close()

def delete_client(client_id):
    conn = connect_db()
    cur = conn.cursor()
    
    cur.execute("DELETE FROM clients WHERE id = %s", (client_id,))
    
    conn.commit()
    cur.close()
    conn.close()

def find_client(first_name=None, last_name=None, email=None, phone=None):
    conn = connect_db()
    cur = conn.cursor()
    
    query = """
    SELECT DISTINCT c.id, c.first_name, c.last_name, c.email
    FROM clients c
    LEFT JOIN phones p ON c.id = p.client_id
    WHERE 1=1
    """
    params = []
    
    if first_name:
        query += " AND c.first_name = %s"
        params.append(first_name)
    if last_name:
        query += " AND c.last_name = %s"
        params.append(last_name)
    if email:
        query += " AND c.email = %s"
        params.append(email)
    if phone:
        query += " AND p.phone = %s"
        params.append(phone)
    
    cur.execute(query, params)
    results = cur.fetchall()
    
    cur.close()
    conn.close()
    return results

#Тест работы функций:
def main():
    print("Добавление клиентов:")
    client1_id = add_client("Федр", "Белов", "fedr@yandex.ru", ["8-926-320-10-15", "8-999-351-68-21"])
    client2_id = add_client("Мария", "Петрова", "maria@yandex.ru", ["8-901-910-55-32"])
    print(f"Добавлены клиенты с ID: {client1_id}, {client2_id}")
    
    print("\nДобавление телефона существующему клиенту:")
    add_phone(client1_id, "8-921-123-45-69")
    print(f"Телефон добавлен клиенту с ID {client1_id}")
    
    print("\nОбновление данных клиента:")
    update_client(client2_id, first_name="Марина", email="marina@yandex.ru")
    print(f"Данные клиента обновлены с ID {client2_id}")

    print("\nУдаление телефона:")
    delete_phone(client1_id, "8-999-351-68-21")
    print(f"Удален телефон у клиента с ID {client1_id}")

    print("\nПоиск клиентов:")
    found_clients = find_client(first_name="Федр")
    for client in found_clients:
        print(f"Найден клиент: {client[1]} {client[2]}, Email: {client[3]}")
        conn = connect_db()
        cur = conn.cursor()
        cur.execute("SELECT phone FROM phones WHERE client_id = %s", (client[0],))
        phones = cur.fetchall()
        cur.close()
        conn.close()
        for phone in phones:
            print(f"  Телефон: {phone[0]}")

    print("\nУдаление клиента:")
    delete_client(client2_id)
    print(f"Удален клиент с ID {client2_id}")

    print("\nПроверка оставшихся клиентов:")
    all_clients = find_client()
    for client in all_clients:
        print(f"Клиент: {client[1]} {client[2]}, Email: {client[3]}")
        conn = connect_db()
        cur = conn.cursor()
        cur.execute("SELECT phone FROM phones WHERE client_id = %s", (client[0],))
        phones = cur.fetchall()
        cur.close()
        conn.close()
        for phone in phones:
            print(f"  Телефон: {phone[0]}")

if __name__ == "__main__":
    create_db()
    main()
