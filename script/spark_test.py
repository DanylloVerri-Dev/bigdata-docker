import logging
from datetime import datetime
from cassandra.auth import PlainTextAuthProvider
from cassandra.cluster import Cluster
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col

# Configurando a sessão spark
spark = SparkSession.builder \
    .appName("Teste_APP") \
    .master("spark://172.22.0.2:7077") \
    .config("spark.cassandra.connection.host", "172.22.0.3") \
    .config("spark.jars.packages", "com.datastax.spark:spark-cassandra-connector_2.12:3.4.1") \
    .getOrCreate()

# Defina os parâmetros do keyspace e da tabela
keyspace_name = "fpso"
table_name = "tabela_test"


# Crie o keyspace usando o driver Python do Cassandra
cluster = Cluster(["172.22.0.3"], auth_provider=PlainTextAuthProvider(username='cassandra', password='cassandra'))
session = cluster.connect()

session.execute(f"CREATE KEYSPACE IF NOT EXISTS {keyspace_name} WITH REPLICATION = {{'class': 'SimpleStrategy', 'replication_factor': 1}};")

# Use o keyspace criado
session.set_keyspace(keyspace_name)

# Crie a tabela de exemplo
session.execute(f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        person_id INT PRIMARY KEY,
        name VARCHAR,
        age INT
    )"""
)


# Criando o dataframe com os dados e o schema
data = [(1, "John", 25), (2, "Jane", 30), (3, "Bob", 22), (4, "Danyllo", 27)]
columns = ["person_id", "name", "age"]
df = spark.createDataFrame(data, columns)

df.show()

# Criando a tabela no cassandra caso não exista, se existir, só insere os dados
df.write \
    .format("org.apache.spark.sql.cassandra") \
    .options(table=table_name, keyspace=keyspace_name) \
    .mode("append") \
    .save()

# Encerre a sessão Spark
spark.stop()


'''
# Crie o keyspace usando o driver Python do Cassandra
cluster = Cluster(["172.22.0.3"], auth_provider=PlainTextAuthProvider(username='cassandra', password='cassandra'))
session = cluster.connect()

session.execute(f"CREATE KEYSPACE IF NOT EXISTS {keyspace_name} WITH REPLICATION = {{'class': 'SimpleStrategy', 'replication_factor': 1}};")

# Use o keyspace criado
session.set_keyspace(keyspace_name)

# Crie a tabela de exemplo
session.execute(f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        id INT PRIMARY KEY,
        nome VARCHAR,
        idade INT
    )
""")

# Insira alguns dados de exemplo na tabela
session.execute(f"INSERT INTO {table_name} (id, nome, idade) VALUES (1, 'Alice', 25)")
session.execute(f"INSERT INTO {table_name} (id, nome, idade) VALUES (2, 'Bob', 30)")
session.execute(f"INSERT INTO {table_name} (id, nome, idade) VALUES (3, 'Charlie', 35)")
session.execute(f"INSERT INTO {table_name} (id, nome, idade) VALUES (4, 'Danyllo', 27)")

# Consulte os dados na tabela
df = spark.read \
    .format("org.apache.spark.sql.cassandra") \
    .options(keyspace=keyspace_name, table=table_name) \
    .load()

df.show()'''

