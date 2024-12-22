import psycopg2
import random
from datetime import datetime, timedelta
import faker


db_params_1 = {
    'dbname': 'project3',
    'user': 'gaussdb',
    'password': 'Fangyoucheng123@',
    'host': 'localhost',
    'port': '15432'
}


db_params_2 = {
    'dbname': 'project3',
    'user': 'postgres',
    'password': '123456',
    'host': 'localhost',
    'port': '5432'
}


fake = faker.Faker()


def create_connection(db_params):
    conn = psycopg2.connect(**db_params)
    cursor = conn.cursor()
    return conn, cursor

def create_table(cursor):
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS test_data(
        id SERIAL PRIMARY KEY,
        name TEXT,
        value INTEGER,
        created_at TIMESTAMP,
        category TEXT,
        price FLOAT,
        quantity INTEGER,
        description TEXT,
        is_active BOOLEAN
    );
    """)
    cursor.connection.commit()


def generate_and_insert_data(cursor, batch_size=10000, total_rows=500000):  # 修改 total_rows=500000
    for batch_start in range(0, total_rows, batch_size):
        insert_query = """
        INSERT INTO test_data (name, value, created_at, category, price, quantity, description, is_active)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s);
        """
        data = []

        for i in range(batch_size):
            name = f'name_{batch_start + i + 1}'  
            value = random.randint(1, 1000) 
            created_at = datetime.now() - timedelta(days=random.randint(0, 100))  # 模拟 created_at 字段
            category = random.choice(['Electronics', 'Clothing', 'Books', 'Sports', 'Toys'])  # 随机分类
            price = round(random.uniform(10.0, 500.0), 2) 
            quantity = random.randint(1, 100)  
            description = fake.sentence(nb_words=15)  
            is_active = random.choice([True, False])  
            data.append((name, value, created_at, category, price, quantity, description, is_active))


        cursor.executemany(insert_query, data)
        cursor.connection.commit()
        print(f'Batch {batch_start // batch_size + 1} inserted.')


def main():

    conn_1, cursor_1 = create_connection(db_params_1)
    create_table(cursor_1)  
    generate_and_insert_data(cursor_1, total_rows=500000) 

 
    conn_2, cursor_2 = create_connection(db_params_2)
    create_table(cursor_2) 
    generate_and_insert_data(cursor_2, total_rows=500000)


    cursor_1.close()
    conn_1.close()
    cursor_2.close()
    conn_2.close()

    print("Data generation and insertion completed into both databases.")
if __name__ == '__main__':
    main()
