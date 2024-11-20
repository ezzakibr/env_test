from cassandra.cluster import Cluster

# Connect to Cassandra
cluster = Cluster(['localhost'])
session = cluster.connect()

# Create keyspace
session.execute("""
    CREATE KEYSPACE IF NOT EXISTS test_keyspace 
    WITH replication = {'class':'SimpleStrategy', 'replication_factor':1}
""")

# Create table
session.execute("""
    CREATE TABLE IF NOT EXISTS test_keyspace.messages (
        id INT PRIMARY KEY,
        timestamp DOUBLE,
        value DOUBLE,
        category TEXT
    )
""")

print("Cassandra setup completed")
cluster.shutdown()