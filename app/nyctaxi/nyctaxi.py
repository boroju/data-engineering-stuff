from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.window import Window

# Create SparkSession
spark = SparkSession.builder \
    .appName("Top Taxi Drivers Example") \
    .getOrCreate()

# Load the CSV data
nyc_taxi_df = spark.read.format("csv").option("header", "true").load("data/30000_taxi_trip_data.csv")

# Define the window specification partitioned by driver_id and ordered by total_amount
window_spec = Window.partitionBy("driver_id").orderBy(F.col("total_amount").desc())

# Apply the rank function within each partition
nyc_taxi_df = nyc_taxi_df.withColumn("rank", F.rank().over(window_spec))

# Filter top 10 ranked taxi drivers
top_taxi_drivers_df = nyc_taxi_df.filter(nyc_taxi_df["rank"] <= 10)

# Select the desired columns
result_df = top_taxi_drivers_df.select("driver_id", "total_amount", "rank")

# Show the result
result_df.show()
