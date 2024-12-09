from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, udf
import requests
import os

spark = SparkSession.builder.appName("Restaurant Geolocation Updater").getOrCreate()    # Initialize the Spark session
GEOCODE_API_KEY = "4f31c2304fc34a77b432d377794f630c"        # API key for OpenCage geocoding

# Get latitude and longitude from Opencage API
def get_coordinates(city_name, country_name):
    if not city_name or not country_name:
        return None, None
    else:
        api_url = f"https://api.opencagedata.com/geocode/v1/json?q={city_name},{country_name}&key={GEOCODE_API_KEY}"
        for attempt in range(2):
            response = requests.get(api_url)
            if response.status_code == 200:
                response_data = response.json()
                if response_data.get("results"):
                    coords = response_data["results"][0]["geometry"]
                    return coords["lat"], coords["lng"]

# User-defined function (UDF) for formatting geolocation
@udf("struct<lat:double,lng:double>")
def format_coordinates(city_name, country_name):
    lat, lng = get_coordinates(city_name, country_name)
    if lat is not None and lng is not None:
        return {"lat": lat, "lng": lng}
    return None

# Read and collect all CSV files
data_directory = "/Users/Admin/IdeaProjects/EpamTask2/restaurant_csv/"
csv_file_list = [os.path.join(data_directory, f) for f in os.listdir(data_directory) if f.endswith(".csv")]
restaurant_data = spark.read.csv(csv_file_list, header=True, inferSchema=True)

# Filter records with missing latitude or longitude
missing_coordinates = restaurant_data.filter((col("lat").isNull()) | (col("lng").isNull()))
print("Records missing latitude or longitude:")
missing_coordinates.show(truncate=False)

# Update rows with missing geolocation by calling the API
updated_missing_data = missing_coordinates.withColumn(
    "geolocation", format_coordinates(col("city"), col("country"))
).withColumn(
    "lat", when(col("geolocation").isNotNull(), col("geolocation.lat")).otherwise(col("lat"))
).withColumn(
    "lng", when(col("geolocation").isNotNull(), col("geolocation.lng")).otherwise(col("lng"))
).drop("geolocation")

# Display updated records
print("Updated records with missing geolocation:")
updated_missing_data.show(truncate=False)

# Combine updated records with existing complete data
existing_coordinates = restaurant_data.filter((col("lat").isNotNull()) & (col("lng").isNotNull()))
final_combined_data = existing_coordinates.union(updated_missing_data)

# Save the resulting dataset
output_directory = "/Users/Admin/IdeaProjects/EpamTask2/restaurant_csv/updated_restaurant_data"
final_combined_data.coalesce(1).write.csv(output_directory, header=True, mode="overwrite")
spark.stop() # End the Spark session

