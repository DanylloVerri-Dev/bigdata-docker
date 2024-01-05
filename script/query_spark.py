from cassandra.auth import PlainTextAuthProvider
from cassandra.cluster import Cluster
from pyspark.sql import SparkSession


# Configurando a sessão spark
spark = SparkSession.builder \
    .appName("Teste_APP") \
    .master("local[2]") \
    .config("spark.cassandra.connection.host", "172.25.0.3") \
    .config("spark.jars.packages", "com.datastax.spark:spark-cassandra-connector_2.12:3.4.1") \
    .getOrCreate()

# Substitua "seu_keyspace" e "sua_tabela" pelos valores apropriados
keyspace = "fpso"


# Criando visões temporárias para as tabelas Cassandra
spark.read.format("org.apache.spark.sql.cassandra") \
    .options(table="equipment_tb", keyspace=keyspace) \
    .load().createOrReplaceTempView("equipment_tb")

spark.read.format("org.apache.spark.sql.cassandra") \
    .options(table="failurelogs_tb", keyspace=keyspace) \
    .load().createOrReplaceTempView("failurelogs_tb")

spark.read.format("org.apache.spark.sql.cassandra") \
    .options(table="sensors_tb", keyspace=keyspace) \
    .load().createOrReplaceTempView("sensors_tb")

query1 = """
    select eqp.name, count(fails.classification) from equipment_tb as eqp
    left join sensors_tb as sen on sen.equipment_id = eqp.equipment_id
    left join failurelogs_tb as fails on sen.sensor_id = fails.sensor
    where fails.classification = "ERROR"
    group by eqp.name
    order by count(fails.classification) desc
"""

result1_df = spark.sql(query1)
print("Total de falhas por equipamentos:")
result1_df.show()
print("Equipamento que mais apresentou falhas: 2C195700 \n")



query2 = """
    with group_fails as (
    select eqp.group_name, count(fails.classification) as total_sensor_fails from equipment_tb as eqp
    left join sensors_tb as sen on sen.equipment_id = eqp.equipment_id
    left join failurelogs_tb as fails on sen.sensor_id = fails.sensor
    where fails.classification = "ERROR"
    group by eqp.group_name
    ),
    qtd_equip_group as (
    select eqp.group_name, count(eqp.equipment_id) as qtd_equipment from equipment_tb as eqp
    group by eqp.group_name
    )
    SELECT gf.group_name, (CAST(gf.total_sensor_fails AS DOUBLE) / CAST(qtd_eqp.qtd_equipment AS DOUBLE)) AS equip_fails_avg 
    FROM group_fails AS gf
    LEFT JOIN qtd_equip_group AS qtd_eqp ON gf.group_name = qtd_eqp.group_name
    ORDER BY equip_fails_avg ASC
"""

result2_df = spark.sql(query2)
print("Média de falhas por grupo de equipamento, ordenado por numero de falhas: \n")
result2_df.show()

query3 = """
        SELECT e.group_name, e.name AS equipment_name, s.sensor_id, COUNT(f.log_id) AS error_count
        FROM equipment_tb e
        JOIN sensors_tb s ON e.equipment_id = s.equipment_id
        LEFT JOIN failurelogs_tb f ON s.sensor_id = f.sensor AND f.classification = 'ERROR'
        GROUP BY e.group_name, e.name, s.sensor_id
        ORDER BY e.group_name, e.name, error_count DESC;
    """
print("Ranking de sensores com maior numero de falhas, por nome de equipamento em um grupo:\n")
result3_df = spark.sql(query3)
result3_df.show()