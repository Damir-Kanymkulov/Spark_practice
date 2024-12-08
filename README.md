# Spark_practice

1. o	Check restaurant data for incorrect (null) values (latitude and longitude). For incorrect values, map latitude and longitude from the OpenCage Geocoding API in a job via the REST API.
Steps:
- PySpark and Requests libraries were imported
- Spark session was created using function from PySpark library.
- My own OpenCage API key was initialized and used in the get_coordinates() function. get_coordinates() function sends a request to the OpenCage API to get latitude and longitude based on the city and country of each restaurant franchise. Connection to the OpenCage Geocoding API was created using its REST API.
- User-defined function (UDF) called fetch_geolocation() applies structured formatting to the output of the get_coordinates() function.
- The restaurant data was loaded from CSV files into a DataFrame via spark.read.csv().
- The latitude and longitude columns were checked whether they contain any incorrect (null) values or not.
- The null latitude and longitude values were replaced with the values returned by the OpenCage Geocoding API.
- The updated data was printed and saved to the CSV file.

