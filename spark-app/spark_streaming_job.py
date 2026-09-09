from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window, count, avg
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType, BooleanType, IntegerType
from pyspark.sql.functions import from_json, col, window, count, avg, to_timestamp

# Step 1: Define the schema matching our Kafka JSON messages
# Why: Spark needs to know the structure upfront to parse JSON efficiently
# and give us typed columns instead of raw strings
flight_schema = StructType([
    StructField("icao24", StringType()),
    StructField("callsign", StringType()),
    StructField("origin_country", StringType()),
    StructField("time_position", LongType()),
    StructField("last_contact", LongType()),
    StructField("longitude", DoubleType()),
    StructField("latitude", DoubleType()),
    StructField("baro_altitude", DoubleType()),
    StructField("on_ground", BooleanType()),
    StructField("velocity", DoubleType()),
    StructField("true_track", DoubleType()),
    StructField("vertical_rate", DoubleType()),
    StructField("geo_altitude", DoubleType()),
    StructField("squawk", StringType()),
    StructField("spi", BooleanType()),
    StructField("position_source", IntegerType()),
])

# Step 2: Create the Spark session
# Why "local[*]": tells Spark to run using all available cores on this machine,
# since we're not running a real multi-node cluster
# Note: the Kafka connector package is NOT set here anymore — it's passed via
# spark-submit --packages on the command line instead (see run instructions)
spark = SparkSession.builder \
    .appName("FlightPositionStreaming") \
    .master("local[*]") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")  # reduce noisy INFO logs in the console

# Step 3: Read the stream from Kafka
# Why "kafka:19092": this is the INTERNAL listener we set up earlier —
# Spark is running inside the same Docker network as Kafka, so it uses
# the internal port, not localhost:9092 (that's only for your Windows scripts)
raw_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:19092") \
    .option("subscribe", "flight-positions") \
    .option("startingOffsets", "latest") \
    .load()

# Step 4: Parse the JSON
# Kafka gives us raw bytes in a column called "value" — we cast it to string,
# then parse it against our schema to get actual structured columns
parsed_stream = raw_stream.select(
    from_json(col("value").cast("string"), flight_schema).alias("data")
).select("data.*")

# Step 5: The actual aggregation — this is the analytical value of the pipeline
# We're counting aircraft and averaging their velocity, grouped by country,
# in 1-minute tumbling windows based on when the position was recorded
from pyspark.sql.functions import to_timestamp

# First, convert the Unix timestamp into an actual timestamp column
parsed_stream = parsed_stream.withColumn(
    "event_time", to_timestamp(col("time_position"))
)

aggregated = parsed_stream \
    .withWatermark("event_time", "2 minutes") \
    .groupBy(
        window(col("event_time"), "1 minute"),
        col("origin_country")
    ) \
    .agg(
        count("*").alias("aircraft_count"),
        avg("velocity").alias("avg_velocity")
    )

from pyspark.sql.functions import col

# Flatten the window struct into two plain timestamp columns,
# since JDBC/Postgres can't represent a struct type directly
aggregated = aggregated.select(
    col("window.start").alias("window_start"),
    col("window.end").alias("window_end"),
    col("origin_country"),
    col("aircraft_count"),
    col("avg_velocity")
)
# Step 6: Write each micro-batch to Postgres using foreachBatch
# Why foreachBatch: Postgres isn't a "native" streaming sink like console/Kafka,
# so Spark gives us this hook to run normal batch-style JDBC writes
# on each micro-batch of the stream
def write_to_postgres(batch_df, batch_id):
    batch_df.write \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://postgres:5432/flightdb") \
        .option("dbtable", "flight_aggregates") \
        .option("user", "flightuser") \
        .option("password", "flightpass") \
        .option("driver", "org.postgresql.Driver") \
        .mode("append") \
        .save()
    print(f"Batch {batch_id} written to Postgres")

query = aggregated.writeStream \
    .outputMode("update") \
    .foreachBatch(write_to_postgres) \
    .trigger(processingTime="30 seconds") \
    .start()

query.awaitTermination()