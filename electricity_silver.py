from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    from_json,
    to_timestamp,
    year,
    month,
    dayofmonth,
    current_timestamp,
    row_number
)
from pyspark.sql.window import Window
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    BooleanType,
    TimestampType
)
import logging

from electricity_bronze import logger

#loging configuration
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s"
)

#spark session
spark = (
    SparkSession.builder
    .appName("ElectricitySilverLayer")
    .config(
        "spark.sql.extensions",
        "io.delta.sql.DeltaSparkSessionExtension"
    )
    .config(
        "spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.delta.catalog.DeltaCatalog"
    )
    .config("spark.sql.shuffle.partitions", "8")
    .getOrCreate()
)

#path configuration
BRONZE_PATH = "/data_lake/bronze/carbon_intensity"
SILVER_DELTA_PATH = "/data_lake/silver/delta/carbon_intensity"
SILVER_PARQUET_PATH = "/data_lake/silver/parquet/carbon_intensity"
BAD_RECORD_PATH = "/data_lake/silver/bad_records/carbon_intensity"

#schema definition
carbon_schema = StructType([
    StructField("zone", StringType(), True),
    StructField("carbonIntensity", IntegerType(), True),
    StructField("datetime", StringType(), True),
    StructField("updatedAt", StringType(), True),
    StructField("createdAt", StringType(), True),
    StructField("emissionFactorType", StringType(), True),
    StructField("isEstimated", BooleanType(), True),
    StructField("estimationMethod", StringType(), True),
    StructField("temporalGranularity", StringType(), True)
])

#read bronze data
logger.info("Reading Bronze data")
bronze_df = spark.read.json(BRONZE_PATH)

#parse raw bronze data
parsed_df = bronze_df.select(
    col("raw_data"),
    col("source_url"),
    col("ingestion_timestamp"),

    from_json(
        col("raw_data"),
        carbon_schema
    ).alias("parsed_json")
)

#bad record handling
bad_records_df = parsed_df.filter(
    col("parsed_json").isNull()
)
if bad_records_df.count() > 0:
    logger.warning("Writing malformed records")
    (
        bad_records_df
        .write
        .mode("append")
        .json(BAD_RECORD_PATH)
    )

#valid record
valid_df = parsed_df.filter(
    col("parsed_json").isNotNull()
)

#Flatten and type conversion
silver_df = valid_df.select(
    col("parsed_json.zone")
    .alias("zone"),
    col("parsed_json.carbonIntensity")
    .cast(IntegerType())
    .alias("carbon_intensity"),
    to_timestamp(
        col("parsed_json.datetime")
    ).alias("event_timestamp"),
    to_timestamp(
        col("parsed_json.updatedAt")
    ).alias("updated_at"),
    to_timestamp(
        col("parsed_json.createdAt")
    ).alias("created_at"),
    col("parsed_json.emissionFactorType")
    .alias("emission_factor_type"),
    col("parsed_json.isEstimated")
    .cast(BooleanType())
    .alias("is_estimated"),
    col("parsed_json.estimationMethod")
    .alias("estimation_method"),
    col("parsed_json.temporalGranularity")
    .alias("temporal_granularity"),
    col("source_url"),
    col("ingestion_timestamp"),
    current_timestamp().alias("silver_load_timestamp")
)

#data quality validation
validated_df = (
    silver_df
    .filter(col("zone").isNotNull())
    .filter(col("event_timestamp").isNotNull())
    .filter(col("carbon_intensity").isNotNull())
)

#deduplication
window_spec = Window.partitionBy(
    "zone",
    "event_timestamp"
).orderBy(
    col("ingestion_timestamp").desc()
)

dedup_df = (
    validated_df
    .withColumn(
        "row_num",
        row_number().over(window_spec)
    )
    .filter(col("row_num") == 1)
    .drop("row_num")
)

#PARTITION COLUMNS
final_df = dedup_df.select(
    "*",
    year("event_timestamp").alias("year"),
    month("event_timestamp").alias("month"),
    dayofmonth("event_timestamp").alias("day")
)

#repartition
final_df = final_df.repartition(
    "year",
    "month",
    "day"
)

#write delta table
logger.info("Writing Silver Delta table")

(
    final_df
    .write
    .format("delta")
    .mode("append")
    .partitionBy("year", "month", "day")
    .save(SILVER_DELTA_PATH)
)

#write parquet
logger.info("Writing Silver Parquet files")
(
    final_df
    .write
    .mode("append")
    .partitionBy("year", "month", "day")
    .parquet(SILVER_PARQUET_PATH)
)
spark.stop()


