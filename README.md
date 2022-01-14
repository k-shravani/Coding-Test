# Spark Coding Assignment

## Prerequisites

- Installed spark 3.2.0 on Ubuntu 18.04.
- Installed Pyspark

## Steps
1. Firstly, I have downloaded NonConfidential.csv and Confidential.snappy.parquet files in Local system.
2. I have created Spark Session using SparkSession.builder.
3. I have read those files into respectively dataframes such as I have read parquet file into one df and csv file into another df using respectively dataframes reader format.
4. Combine those 2 files into a single entity i.e combined_df.
5. Then, Performed data cleansing operations like dropping null values, casting and filtering.
6. Create a Temporary View on combined_df.
7. Fired that all queries on that Temporary view.
8. Save the results in tmp/output/ directory as per the question answers.

## Execution
- For execution of spark coding assignment, I have created bash script.
- You can execute that script using following command
```shell
    bash bash_script.sh
```