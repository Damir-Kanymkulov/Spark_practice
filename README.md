# Spark_practice

1. Check restaurant data for incorrect (null) values (latitude and longitude). For incorrect values, map latitude and longitude from the OpenCage Geocoding API in a job via the REST API.
Steps:
- pyspark.sql.SparkSession, pyspark.sql.functions, requests, and os libraries were imported.
- Spark session was created using pyspark.sql.SparkSession library.
- My own OpenCage Geocoding API key was initialized and used in the get_coordinates() function.
- get_coordinates() function sends a request to the OpenCage API to get latitude and longitude based on the city and country of each restaurant franchise. Connection to the OpenCage Geocoding API was created using its REST API.
- User-defined function (UDF) called format_coordinates() applies structured formatting to the output of the get_coordinates() function. It uses get_coordinates() function, takes its output and returns a structured object with latitude and longitude.
- The folder with all restaurant CSV files was scanned and all files were collected into a list of file paths. The restaurant data was loaded from CSV files into a DataFrame via spark.read.csv().
- The latitude and longitude columns were checked to see whether they contain any incorrect (null) values or not. Rows with missing coordinates were identified using isNull() function. The null latitude and longitude values were replaced with the values returned by the OpenCage Geocoding API. The UDF format_coordinates() function was applied to get formatted latitude and longitude for rows with missing coordinates.
- The updated data was printed and saved to the CSV file.
- SparkSession was stopped at the end.

2. Generate a geohash by latitude and longitude using a geohash library like geohash-java. Your geohash should be four characters long and placed in an extra column.
Steps:
- pyspark.sql.SparkSession, pyspark.sql.functions for udf and col, pyspark.sql.types for StringType, os, and pygeohash libraries were imported.
- Spark session was initialized using pyspark.sql.SparkSession library.
- A function to compute geohash was defined. It is called compute_geohash(). This function takes latitude and longitude as input and returns a four-character geohash using pygeohash.encode() function.
- The compute_geohash() function was registered as a UDF function for Spark DataFrame operations.
- The corrected restaurant data was loaded from CSV file into a DataFrame via spark.read.csv().
- A new geohash column was added to the restaurant DataFrame.
- The updated data was saved to the parquet file.
- SparkSession was stopped at the end.

After generating a four-character geohash for corrected restaurant data, the same procedure was done to generate the four-character geohash for the weather data. All weather parquet files were combined and loaded using get_all_parquet_files() and spark.read.parquet() functions. Here, get_all_parquet_files() was defined to list all weather parquet files within the directory.
