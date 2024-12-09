from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Join Restaurant and Weather Data").getOrCreate() # Create a Spark session

# Load the Parquet files into Spark DataFrames
weather_dataset_path = "/Users/Admin/IdeaProjects/EpamTask2/weather_dataset/weather_data_with_geohash"
weather_data = spark.read.parquet(weather_dataset_path)
restaurant_dataset_path = "/Users/Admin/IdeaProjects/EpamTask2/restaurant_parquet/restaurant_data_with_geohash"
restaurant_data = spark.read.parquet(restaurant_dataset_path)

# Left join the weather and restaurant data using the four-character geohash
enriched_data = restaurant_data.join(weather_data, on="geohash", how="left")
enriched_data.show(n=100, truncate=False)   # Display the output DataFrame

# Save the enriched dataset, partitioned by geohash
output_directory = "/Users/Admin/IdeaProjects/EpamTask2/joined_data"
enriched_data.write.partitionBy("geohash").mode("overwrite").parquet(output_directory)
spark.stop()    # Stop the Spark session