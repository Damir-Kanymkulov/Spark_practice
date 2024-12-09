from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.types import StringType
import pygeohash as geohash_lib
import os

spark = SparkSession.builder.appName("Geohash Generator for Weather").getOrCreate() # Start a Spark session

# Function to compute geohash based on latitude and longitude
def compute_geohash(latitude, longitude):
    if latitude is not None and longitude is not None:
        return geohash_lib.encode(latitude, longitude, precision=4)  # Generate geohash with 4-char precision
    return None

# Register the geohash computation function as a UDF
geohash_function = udf(compute_geohash, StringType())
weather_data_dir = "/Users/Admin/IdeaProjects/EpamTask2/weather_dataset/weather1"

# List all weather parquet files within the directory
def get_all_parquet_files(directory_path):
    all_parquet_files = []
    for root, dirs, files in os.walk(directory_path):
        for file in files:
            if file.endswith(".parquet"):
                all_parquet_files.append(os.path.join(root, file))
    return all_parquet_files

# Load and combine all the weather parquet files into a single DataFrame
parquet_file_list = get_all_parquet_files(weather_data_dir)
weather_data_df = spark.read.parquet(*parquet_file_list)

# Add a column with geohash values computed from latitude and longitude
updated_weather_data = weather_data_df.withColumn("geohash", geohash_function(col("lat"), col("lng")))

# Define the output path for the combined and updated dataset
output_directory = "/Users/Admin/IdeaProjects/EpamTask2/weather_dataset/weather_data_with_geohash"
updated_weather_data.coalesce(1).write.mode("overwrite").parquet(output_directory)
spark.stop()    # Stop the Spark session