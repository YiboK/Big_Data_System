# Big_Data_System
This repository is dedicated to the course project for CS544 Intro to Big Data Systems at UW-Madison, where we explored key concepts and technologies used in managing and analyzing large-scale data. The course covers:

- Distributed Systems Deployment: Learn how to set up and manage distributed systems using ***Docker*** and ***Hadoop***, including concepts like resource management, caching, and thread optimization.

- Data Storage Solutions: Understand both structured and unstructured storage approaches, from traditional file systems and SQL databases to NoSQL systems like ***Cassandra*** and ***HDFS***.

- Big Data Processing: Use frameworks like ***Apache Spark*** for parallel processing, ***MapReduce***, and data manipulation through ***Spark SQL*** and DataFrames.

- Streaming Data: Dive into real-time data processing with ***Kafka*** and ***Spark Streaming*** to analyze and respond to data as it arrives.

- Machine Learning Models: Train and optimize machine learning models with ***PyTorch***, applying distributed computing techniques to handle large datasets.

This project will integrate these technologies, focusing on real-world applications of distributed data systems to solve complex problems at scale. All necessary files, scripts, and documentation can be found within this repository.

Here is a list of what topics each project covers:

### Project 1:

In this project, we used PyTorch to build a regression model that predicts COVID-19 deaths in Wisconsin census tracts based on positive cases by age group. The project involved creating and optimizing tensors, utilizing GPUs for computations, and calculating the R² score for model accuracy.

### Project 2:

In this project, we built a gRPC-based key/value store service that supports concurrent requests using threads and locking for safe access. We implemented caching for factorial calculations to improve performance and measured key statistics like cache hit rate and response times. Finally, the project was containerized using Docker for easy deployment.

### Project 3:

In this project, we deployed an HDFS cluster using Docker and experimented with file replication and fault tolerance. We uploaded large files to HDFS with different replication settings, then used Python to read the data using the webhdfs API. We also simulated data loss by shutting down a datanode and modified our code to recover as much data as possible from the damaged file.

### Project 4:

In this project, we used Apache Spark to analyze loan applications in Wisconsin by loading data into Hive tables and performing complex queries involving joins, filtering, grouping, and windowing. We explored performance optimization techniques such as caching and bucketing to improve query efficiency. Additionally, we evaluated the impact of caching on load balance and measured query execution times.

### Project 5:

In this project, we set up a Cassandra cluster to store weather data from NOAA, designed a schema with partition and cluster keys, and implemented a gRPC-based server to collect and retrieve temperature data. We also used Apache Spark to analyze the collected data and registered custom UDFs for temperature conversions. Finally, we explored the impact of node failure on the system’s ability to write and read data, ensuring high availability for writes with a focus on consistency for reads.

### Project 6:

In this project, we used Kafka and Spark Streaming to simulate weather data collection and analysis from multiple weather stations. We created Kafka producers and consumers to stream real-time data, ensuring exactly-once semantics for reliable message processing. The Spark streaming job consumed the data to generate datasets and train machine learning models to predict future weather conditions, with results displayed in real time. Finally, we deployed the trained model to stream predictions for specific weather stations.

### Project 7:


In this project, we used Google BigQuery to analyze loan data in Wisconsin, combining public datasets, private Google Sheets data, and a parquet file uploaded to Google Cloud Storage. We applied spatial joins to associate loans with counties and created a machine learning model to predict loan amounts based on income and loan term. Additionally, we learned how to connect BigQuery to external data sources like Google Sheets and used linear regression to evaluate the relationships between variables in the dataset.