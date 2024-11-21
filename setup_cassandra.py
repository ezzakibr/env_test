from cassandra.cluster import Cluster


cluster = Cluster(['localhost'])
session = cluster.connect()

session.set_keyspace('test_keyspace')

session.execute("""
    CREATE TABLE IF NOT EXISTS categories (
        code TEXT PRIMARY KEY,
        name TEXT
    )
""")

session.execute("INSERT INTO categories (code, name) VALUES ('A', 'Category A')")
session.execute("INSERT INTO categories (code, name) VALUES ('B', 'Category B')")
session.execute("INSERT INTO categories (code, name) VALUES ('C', 'Category C')")

session.execute("""
    CREATE TABLE IF NOT EXISTS messages_with_categories (
        id INT PRIMARY KEY,
        date TIMESTAMP,
        value DOUBLE,
        category TEXT,
        category_name TEXT
    )
""")

print("Table 'messages_with_categories' created successfully")
cluster.shutdown()

print("Categories table created and populated")
cluster.shutdown()
