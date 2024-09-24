import logging
from cassandra.cluster import Cluster
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, when
from pyspark.sql.types import StructType, StructField, StringType, FloatType, TimestampType
from pyspark.sql import Window
from pyspark.sql.functions import hour, unix_timestamp, count, when, lag

# Initialize logging
logging.basicConfig(level=logging.INFO)

def create_keyspace(session):
    session.execute("""
        CREATE KEYSPACE IF NOT EXISTS spark_streams
        WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'};
    """)
    logging.info("Keyspace created successfully!")

def create_table(session):
    session.execute("""
    CREATE TABLE IF NOT EXISTS spark_streams.created_users (
        id UUID PRIMARY KEY,
        first_name TEXT,
        last_name TEXT,
        gender TEXT,
        address TEXT,
        post_code TEXT,
        email TEXT,
        username TEXT,
        registered_date TEXT,
        phone TEXT,
        picture TEXT,
        transaction_amount FLOAT,
        currency TEXT,
        transaction_status TEXT,
        product_id TEXT,
        product_category TEXT,
        shipping_address TEXT,
        geolocation TEXT,
        device_type TEXT,
        ip_address TEXT,
        anomaly_flag TEXT
    );
    """)
    logging.info("Table created successfully!")

def create_spark_connection():
    s_conn = SparkSession.builder \
        .appName('SparkDataStreaming') \
        .config('spark.jars.packages', "com.datastax.spark:spark-cassandra-connector_2.13:3.4.1," \
                                         "org.apache.spark:spark-sql-kafka-0-10_2.13:3.4.1") \
        .config('spark.cassandra.connection.host', 'localhost') \
        .getOrCreate()
    s_conn.sparkContext.setLogLevel("ERROR")
    logging.info("Spark connection created successfully!")
    return s_conn

def connect_to_kafka(spark_conn):
    spark_df = spark_conn.readStream \
        .format('kafka') \
        .option('kafka.bootstrap.servers', 'localhost:9092') \
        .option('subscribe', 'users_created') \
        .option('startingOffsets', 'earliest') \
        .load()
    logging.info("Kafka DataFrame created successfully")
    return spark_df

def create_cassandra_connection():
    try:
        # connecting to the cassandra cluster
        cluster = Cluster(['localhost'])

        cas_session = cluster.connect()

        return cas_session
    except Exception as e:
        logging.error(f"Could not create cassandra connection due to {e}")
        return None

def create_selection_df_from_kafka(spark_df):
    schema = StructType([
        StructField("id", StringType(), False),
        StructField("first_name", StringType(), False),
        StructField("last_name", StringType(), False),
        StructField("gender", StringType(), False),
        StructField("address", StringType(), False),
        StructField("post_code", StringType(), False),
        StructField("email", StringType(), False),
        StructField("username", StringType(), False),
        StructField("registered_date", StringType(), False),
        StructField("phone", StringType(), False),
        StructField("picture", StringType(), False),
        StructField("transaction_amount", FloatType(), False),
        StructField("currency", StringType(), False),
        StructField("transaction_status", StringType(), False),
        StructField("product_id", StringType(), False),
        StructField("product_category", StringType(), False),
        StructField("shipping_address", StringType(), False),
        StructField("geolocation", StringType(), False),
        StructField("device_type", StringType(), False),
        StructField("ip_address", StringType(), False)
    ])

    sel = spark_df.selectExpr("CAST(value AS STRING)") \
        .select(from_json(col('value'), schema).alias('data')).select("data.*")
    logging.info("Selection DataFrame created from Kafka DataFrame.")
    return sel


def detect_anomalies(df):
    # Threshold Rules
    # Define thresholds for transaction amount and number of transactions per customer
    amount_threshold = 1000  # Example threshold for high transaction amount
    transaction_count_threshold = 5  # Example threshold for number of transactions in 1 hour

    # Time-Based Rules
    # Identify odd hours (e.g., transactions made between midnight and 6 AM)
    odd_hour_start = 0  # Midnight
    odd_hour_end = 6  # 6 AM

    # Geolocation Anomalies
    # For this example, we will assume `geolocation` is a string containing latitude and longitude
    # This is a simplistic check; you might need a more complex geolocation check based on your requirements.
    
    # Create a window to count transactions per customer
    window_spec = Window.partitionBy("username").orderBy("registered_date").rowsBetween(-1, 0)

    # Detect anomalies based on defined rules
    df = df.withColumn("anomaly_flag",
        when(col("transaction_amount") > amount_threshold, "High Amount")
        .when(col("transaction_amount") < 0, "Negative Amount")
        .when((hour(col("registered_date")) >= odd_hour_start) & (hour(col("registered_date")) < odd_hour_end), "Odd Hour Transaction")
        .when(col("geolocation").isNull(), "Missing Geolocation")
        .when(count("id").over(window_spec) > transaction_count_threshold, "Too Many Transactions in Short Time")
        .otherwise("Normal")
    )

    # Geolocation Anomalies: Spotting inconsistent locations
    # Here we assume geolocation is a string format like "latitude,longitude"
    df = df.withColumn("previous_geolocation", lag(col("geolocation")).over(window_spec))
    
    df = df.withColumn("geolocation_anomaly",
        when(
            (col("geolocation") != col("previous_geolocation")) & (col("geolocation").isNotNull() & col("previous_geolocation").isNotNull()), 
            "Inconsistent Location"
        ).otherwise("Consistent Location")
    )

    # Update anomaly flag based on geolocation anomaly
    df = df.withColumn("anomaly_flag",
        when(col("geolocation_anomaly") == "Inconsistent Location", "Geolocation Anomaly")
        .otherwise(col("anomaly_flag"))
    )

    # Pattern Recognition: Detect strange purchasing behaviors
    # For simplicity, we will flag bulk buying of the same product
    df = df.withColumn("previous_product_id", lag(col("product_id")).over(window_spec))
    df = df.withColumn("pattern_anomaly",
        when(
            (col("product_id") == col("previous_product_id")) & (count("id").over(window_spec) > 3),
            "Bulk Buying Anomaly"
        ).otherwise("Normal Pattern")
    )

    # Finalize anomaly flag based on pattern recognition
    df = df.withColumn("anomaly_flag",
        when(col("pattern_anomaly") == "Bulk Buying Anomaly", "Bulk Buying Anomaly")
        .otherwise(col("anomaly_flag"))
    )

    logging.info("Anomalies detected based on predefined rules.")
    return df


def main():
    # Create Spark connection
    spark_conn = create_spark_connection()
    if spark_conn is not None:
        # Connect to Kafka
        spark_df = connect_to_kafka(spark_conn)
        selection_df = create_selection_df_from_kafka(spark_df)

        # Detect anomalies in the selection DataFrame
        flagged_df = detect_anomalies(selection_df)

        # Create Cassandra connection
        session = create_cassandra_connection()
        if session is not None:
            create_keyspace(session)
            create_table(session)

            # Write to Cassandra
            streaming_query = (flagged_df.writeStream
                               .format("org.apache.spark.sql.cassandra")
                               .option('checkpointLocation', '/tmp/checkpoint')
                               .option('keyspace', 'spark_streams')
                               .option('table', 'created_users')
                               .start())

            logging.info("Streaming query started. Awaiting termination...")
            streaming_query.awaitTermination()

if __name__ == "__main__":
    main()
