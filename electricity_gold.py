from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    to_date,
    avg,
    max,
    min,
    count,
    sum,
    when,
    round,
    year,
    month,
    dayofmonth
)

#spark session
spark = (
    SparkSession.builder
    .appName("ElectricityGoldLayer")
    .getOrCreate()
)

#Input silver Data
SILVER_PATH = "/data_lake/silver/delta/carbon_intensity"
GOLD_PATH = "/data_lake/gold"
df = spark.read.format("delta").load(SILVER_PATH)

#add date column
df = df.withColumn("event_date", to_date("event_timestamp")
)

#Data product 1-----------
# Daily total per zone
daily_total = df.groupBy("zone", "event_date").agg(
    sum("carbon_intensity").alias("daily_total_ci")
)

#Join back to compute relative contribution
joined = df.join(
    daily_total,
    ["zone", "event_date"],
    "inner"
)
relative_mix = joined.withColumn(
    "relative_contribution_pct",
     round((col("carbon_intensity") / col("daily_total_ci")) * 100, 2)
)

#final gold table
gold_mix = relative_mix.select(
    "zone",
    "event_timestamp",
    "carbon_intensity",
    "daily_total_ci",
    "relative_contribution_pct",
    "event_date"
).withColumn("year", year("event_date")) \
    .withColumn("month", month("event_date")) \
    .withColumn("day", dayofmonth("event_date"))

#write delta
gold_mix.write \
    .format("delta") \
    .mode("overwrite") \
    .partitionBy("year", "month", "day") \
    .save(f"{GOLD_PATH}/daily_relative_mix_delta")

#write parquet
gold_mix.write \
    .mode("overwrite") \
    .partitionBy("year", "month", "day") \
    .parquet(f"{GOLD_PATH}/daily_relative_mix_parquet")

spark.stop()