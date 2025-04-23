import psycopg2
import csv

conn = psycopg2.connect(
    dbname="postgres",
    user="postgres",
    host="localhost",
    port="5432"
)
cur = conn.cursor()

cur.execute("""
CREATE TABLE IF NOT EXISTS phonebook (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    phone VARCHAR(20) NOT NULL
);
""")
conn.commit()

def create_stored_procedures():
    cur.execute("""
    CREATE OR REPLACE PROCEDURE insert_or_update_user(name_input VARCHAR, phone_input VARCHAR)
    LANGUAGE plpgsql AS $$
    BEGIN
        IF EXISTS (SELECT 1 FROM phonebook WHERE name = name_input) THEN
            UPDATE phonebook SET phone = phone_input WHERE name = name_input;
        ELSE
            INSERT INTO phonebook (name, phone) VALUES (name_input, phone_input);
        END IF;
    END;
    $$;
    """)

    cur.execute("""
    CREATE OR REPLACE FUNCTION search_by_pattern(pattern VARCHAR)
    RETURNS TABLE(id INT, name VARCHAR, phone VARCHAR)
    LANGUAGE plpgsql AS $$
    BEGIN
        RETURN QUERY SELECT * FROM phonebook
        WHERE name ILIKE '%' || pattern || '%' OR phone ILIKE '%' || pattern || '%';
    END;
    $$;
    """)

    cur.execute("""
    CREATE OR REPLACE FUNCTION get_paginated_users(lim INT, offs INT)
    RETURNS TABLE(id INT, name VARCHAR, phone VARCHAR)
    LANGUAGE plpgsql AS $$
    BEGIN
        RETURN QUERY SELECT * FROM phonebook ORDER BY id LIMIT lim OFFSET offs;
    END;
    $$;
    """)

    cur.execute("""
    CREATE OR REPLACE PROCEDURE delete_user_by_name_or_phone(value VARCHAR)
    LANGUAGE plpgsql AS $$
    BEGIN
        DELETE FROM phonebook WHERE name = value OR phone = value;
    END;
    $$;
    """)
    conn.commit()

create_stored_procedures()

def insert_from_csv(filename):
    with open(filename, 'r') as file:
        reader = csv.DictReader(file)
        for row in reader:
            cur.execute("CALL insert_or_update_user(%s, %s)", (row['name'], row['phone']))
    conn.commit()


def insert_or_update_input():
    name = input("Имя: ")
    phone = input("Телефон: ")
    cur.execute("CALL insert_or_update_user(%s, %s)", (name, phone))
    conn.commit()

def update_data():
    field = input("Что изменить? (name/phone): ")
    value = input("Что обновить: ")
    new_value = input("Новое значение: ")
    if field == "name":
        cur.execute("UPDATE phonebook SET name = %s WHERE phone = %s", (new_value, value))
    elif field == "phone":
        cur.execute("UPDATE phonebook SET phone = %s WHERE name = %s", (new_value, value))
    conn.commit()

def search_pattern():
    pattern = input("Введите шаблон для поиска (имя или номер): ")
    cur.execute("""
        SELECT * FROM phonebook 
        WHERE name ILIKE %s OR phone ILIKE %s
    """, (f"%{pattern}%", f"%{pattern}%"))
    results = cur.fetchall()
    for row in results:
        print(row)


def delete_entry():
    value = input("Введите имя или телефон для удаления: ")
    cur.callproc("delete_user_by_name_or_phone", (value,))
    conn.commit()

def show_paginated():
    limit = int(input("Сколько записей показать: "))
    offset = int(input("С какого отступа начать: "))
    cur.execute("SELECT * FROM get_paginated_users(%s, %s)", (limit, offset))
    for row in cur.fetchall():
        print(row)

while True:
    print("\n1. Загрузить из CSV")
    print("2. Ввести вручную")
    print("3. Обновить данные")
    print("4. Поиск по шаблону")
    print("5. Удалить")
    print("6. Показать с пагинацией")
    print("7. Выход")
    choice = input("Выберите: ")

    if choice == '1':
        file = input("Имя CSV-файла: ")
        insert_from_csv(file)
    elif choice == '2':
        insert_or_update_input()
    elif choice == '3':
        update_data()
    elif choice == '4':
        search_pattern()
    elif choice == '5':
        delete_entry()
    elif choice == '6':
        show_paginated()
    elif choice == '7':
        break
    else:
        print("Неверный выбор!")

cur.close()
conn.close()
