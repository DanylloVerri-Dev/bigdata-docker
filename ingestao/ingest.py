import logging
import json
from cassandra.auth import PlainTextAuthProvider
from cassandra.cluster import Cluster
from pyspark.sql import SparkSession
from pyspark.sql.functions import when, col, monotonically_increasing_id, concat_ws, split, regexp_replace, to_timestamp, lpad, regexp_extract
from pyspark.sql.types import StructType, StructField, IntegerType
from pyspark.sql import Window
from pyspark.sql import functions as F

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class CassandraConnector:
    def __init__(self, hosts, keyspace, username, password):
        self.cluster = Cluster(hosts, auth_provider=PlainTextAuthProvider(username=username, password=password))
        self.session = self.cluster.connect()
        self.session.set_keyspace(keyspace)
        logger.info("Connected to Cassandra")

    def create_table_sensors(self):
        query = f"""
            CREATE TABLE IF NOT EXISTS sensors_tb (
                sensor_id INT PRIMARY KEY,
                equipment_id INT
            )"""
        self.session.execute(query)
        logger.info(f"Tabela sensors_tb criada, ou já existe no Cassandra")

    def create_table_equipment(self):
        query = f"""
            CREATE TABLE IF NOT EXISTS equipment_tb (
                equipment_id INT PRIMARY KEY,
                name VARCHAR,
                group_name VARCHAR
            )"""
        self.session.execute(query)
        logger.info(f"Tabela equipment_tb criada, ou já existe no Cassandra")

    def create_table_failurelogs(self):
        query = f"""
            CREATE TABLE IF NOT EXISTS failurelogs_tb (
                log_id INT PRIMARY KEY,
                timestamp TIMESTAMP,
                classification VARCHAR,
                sensor INT,
                temperature VARCHAR,
                vibration VARCHAR
            )"""
        self.session.execute(query)
        logger.info(f"Tabela failurelogs_tb criada, ou já existe no Cassandra")

class SparkDataProcessor:
    def __init__(self, master, cassandra_hosts, keyspace, table_name):
        self.spark = SparkSession.builder \
            .appName("ingest_app") \
            .master(master) \
            .config("spark.cassandra.connection.host", cassandra_hosts) \
            .config("spark.jars.packages", "com.datastax.spark:spark-cassandra-connector_2.12:3.4.1") \
            .config("spark.dynamicAllocation.enabled", "true") \
            .config("spark.dynamicAllocation.maxExecutors", "4") \
            .config("spark.executor.memory", "4g") \
            .config("spark.executor.cores", "2") \
            .getOrCreate()

        self.keyspace = keyspace
        self.table_name = table_name

    def read_json_to_dataframe(self, file_path):
        with open(file_path, "r") as arquivo:
            dados_json = json.load(arquivo)

        df = self.spark.createDataFrame(dados_json)
        
        logger.info(f"Lendo arquivo JSON do caminho {file_path}")
        return df

    def read_csv_to_dataframe(self, file_path):
        schema_csv = StructType([
            StructField("equipment_id", IntegerType(), True),
            StructField("sensor_id", IntegerType(), True),
        ])
        df = self.spark.read.csv(file_path, header=True, schema=schema_csv)
        logger.info(f"Lendo arquivo CSV do caminho {file_path}")
        return df

    def read_txt_to_dataframe(self, file_path):
        df_logs = self.spark.read.option("delimiter", "\\t").csv(file_path)

        df_logs = df_logs.withColumn("classification", df_logs["_c1"])

        df_logs = df_logs.withColumn("full_string", concat_ws(" ", "_c3", "_c4", "_c5"))

        df_logs = df_logs.withColumn("temperature", split(split(df_logs["full_string"], ",")[0], " ")[1])

        df_logs = df_logs.withColumn("vibration", split(split(df_logs["full_string"], ",")[1], " ")[2]) \
                         .withColumn("vibration", regexp_replace("vibration", r"\)", ""))

        df_logs = df_logs.withColumn("timestamp", regexp_replace(col("_c0"), "[\\[\\]]", ""))
        df_logs = df_logs.withColumn("timestamp", regexp_replace(col("timestamp"), "[\\/]", "-"))

        df_logs = df_logs.withColumn("date", split(df_logs["timestamp"], " ")[0]) \
                         .withColumn("time", split(df_logs["timestamp"], " ")[1])
        df_logs = df_logs.withColumn("hour", lpad(split(df_logs["time"], ":")[0], 2, "0")) \
                         .withColumn("min", lpad(split(df_logs["time"], ":")[1], 2, "0")) \
                         .withColumn("sec", lpad(split(df_logs["time"], ":")[2], 2, "0"))
        df_logs = df_logs.withColumn("time", concat_ws(":", "hour", "min", "sec")) \
                         .withColumn("time", when((col("time").isNull()) | (col("time") == ""), "00:00:00").otherwise(col("time")))

        df_logs = df_logs.withColumn("day", split(df_logs["date"], "-")[2]) \
                         .withColumn("day_treatment", lpad(col("day"), 2, "0"))
        df_logs = df_logs.withColumn("date", col("date").substr(1, 8)).withColumn("date", concat_ws("", "date", "day_treatment"))

        df_logs = df_logs.withColumn("timestamp", concat_ws(" ", "date", "time"))

        df_logs = df_logs.withColumn("sensor", regexp_extract(df_logs["_c2"], r"\[(.*?)\]", 1))

        df_logs = df_logs.withColumn("timestamp", to_timestamp(col("timestamp"), "yyyy-MM-dd HH:mm:ss"))

        window_spec = Window.orderBy(F.monotonically_increasing_id())
        df_logs = df_logs.dropDuplicates().withColumn("log_id", F.row_number().over(window_spec) - 1) \
                         .select("log_id", "timestamp", "classification", "sensor", "temperature", "vibration")


        return df_logs

    def write_dataframe_to_cassandra(self, dataframe, table_name):
        dataframe.write \
            .format("org.apache.spark.sql.cassandra") \
            .options(table=table_name, keyspace=self.keyspace) \
            .option("confirm.truncate","true") \
            .option("batch.size.bytes", "1024") \
            .mode("overwrite") \
            .save()
        logger.info(f"DataFrame foi gravado na tabela {self.table_name} do Cassandra")

    def read_cassandra_table(self):
        df = self.spark.read \
            .format("org.apache.spark.sql.cassandra") \
            .options(keyspace=self.keyspace, table=self.table_name) \
            .load()
        logger.info(f"Lendo os dados do Cassandra tabela {self.table_name} e armazenando no dataframe.")
        return df

def main():
    cassandra_connector = CassandraConnector(["172.25.0.3"], "fpso", "cassandra", "cassandra")
    cassandra_connector.create_table_sensors()
    cassandra_connector.create_table_equipment()
    cassandra_connector.create_table_failurelogs()

    spark_processor = SparkDataProcessor("local[2]", "172.25.0.3", "fpso", "sensors_tb")

    file_path_csv = "/home/danyllo/repositories/bigdata-docker/data/equipment_sensors.csv"
    file_path_json = "/home/danyllo/repositories/bigdata-docker/data/equipment.json"
    file_path_txt = "/home/danyllo/repositories/bigdata-docker/data/equpment_failure_sensors.txt"

    df_equipment = spark_processor.read_json_to_dataframe(file_path_json)
    df_sensor = spark_processor.read_csv_to_dataframe(file_path_csv)
    df_log = spark_processor.read_txt_to_dataframe(file_path_txt)

    spark_processor.write_dataframe_to_cassandra(df_sensor, "sensors_tb")
    spark_processor.write_dataframe_to_cassandra(df_equipment, "equipment_tb")
    spark_processor.write_dataframe_to_cassandra(df_log, "failurelogs_tb")


if __name__ == "__main__":
    main()