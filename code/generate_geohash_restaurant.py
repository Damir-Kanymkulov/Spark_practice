from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType
import pygeohash as pgh

spark = SparkSession.builder.appName("Geohash Generator").getOrCreate()  # Start a Spark session

# Function to compute geohash based on latitude and longitude
def compute_geohash(latitude, longitude):
    if latitude is not None and longitude is not None:
        return pgh.encode(latitude, longitude, precision=4)  # Generate geohash with 4-character precision
    return None

# Register the function as a UDF for Spark DataFrame operations
geohash_function = udf(compute_geohash, StringType())

# Load the dataset into a Spark DataFrame
input_file_path = "/Users/Admin/IdeaProjects/EpamTask2/restaurant_csv/updated_combined_restaurant_data/part-00000-8f0da171-d2ef-484d-aca7-5607f3a09c06-c000.csv"
restaurant_data = spark.read.csv(input_file_path, header=True, inferSchema=True)

# Add a new column to the DataFrame for the geohash values
data_with_geohash = restaurant_data.withColumn("geohash", geohash_function(restaurant_data["lat"], restaurant_data["lng"]))

#Save the resulting DataFrame with the geohash column as a Parquet file
output_directory_path = "/Users/Admin/IdeaProjects/EpamTask2/restaurant_parquet/restaurant_data_with_geohash"
data_with_geohash.write.parquet(output_directory_path, mode="overwrite")
spark.stop()    # End the Spark session