# Spark_practice

1. Check restaurant data for incorrect (null) values (latitude and longitude). For incorrect values, map latitude and longitude from the OpenCage Geocoding API in a job via the REST API.
Steps:
- pyspark.sql.SparkSession, pyspark.sql.functions, requests, os, and time libraries were imported
- Spark session was created using pyspark.sql.SparkSession library.
- My own OpenCage Geocoding API key was initialized and used in the get_coordinates() function.
- get_coordinates() function sends a request to the OpenCage API to get latitude and longitude based on the city and country of each restaurant franchise. Connection to the OpenCage Geocoding API was created using its REST API.
- User-defined function (UDF) called fetch_geolocation() applies structured formatting to the output of the get_coordinates() function. It uses get_coordinates() function, takes its output and returns a structured object with latitude and longitude.
- The folder with all restaurant CSV files was scanned and all files were collected into a list of file paths. The restaurant data was loaded from CSV files into a DataFrame via spark.read.csv().
- The latitude and longitude columns were checked whether they contain any incorrect (null) values or not. Rows with missing coordinates were identified using isNull() function. The null latitude and longitude values were replaced with the values returned by the OpenCage Geocoding API. The UDF fetch_geolocation() function was applied to get formatted latitude and longitude for rows with missing coordinates
- The updated data was printed and saved to the CSV file.
- SparkSession was stopped at the end.

