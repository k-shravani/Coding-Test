#!/usr/bin/python3

from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_replace
from pyspark.sql.types import DoubleType

# Creating SparkSession

spark = SparkSession.builder \
    .config("spark.sql.shuffle.partitions", "20") \
    .appName("dataeaze_spark_coding_assignment") \
    .master("local[*]") \
    .getOrCreate()

# reading confidential parquet file
confidential_df = spark.read \
    .parquet("./confidential.snappy.parquet")

# confidential_df.show(10)
# confidential_df.printSchema()
# print(confidential_df.count())

# print("_" * 100)

# reading non_confidential csv file
non_confidential_df = spark.read \
    .option("header", "true") \
    .option("delimiter", ",") \
    .option("inferSchema", "true") \
    .csv("./nonConfidential.csv")

# non_confidential_df.show(10)
# non_confidential_df.printSchema()
# print(non_confidential_df.count())

# Combine these 2 Dataframes into a single entity
combine_df = non_confidential_df.union(confidential_df)

# print(combine_df.count())
# combine_df.show(10)
# combine_df.printSchema()

# Dropping null values
combine_df_2 = combine_df.na.drop(subset=["Zipcode", "PointsAchieved", "GrossSqFoot"])

# print(combine_df.count())
# print(combine_df_2.count())

combine_df_3 = combine_df_2\
    .withColumn('GrossSqFoot', combine_df['GrossSqFoot'].cast(DoubleType())) \
    .withColumn('PointsAchieved', regexp_replace('PointsAchieved', '[^0-9]', '0')) \

final_df = combine_df_3.filter(combine_df_3['Zipcode'] != "\\\"\\\"")

final_df.show(50)

# Create temporary view on combine_df
final_df.createOrReplaceTempView("final_view")

# question_1 : How many LEED projects are there in Virginia (including all types of project types and versions of LEED)?
# question_1 = "SELECT ProjectTypes, LEEDSystemVersionDisplayName, COUNT(LEEDSystemVersionDisplayName) FROM final_view WHERE State='VA' GROUP BY LEEDSystemVersionDisplayName, ProjectTypes"
question_1 = "SELECT COUNT(LEEDSystemVersionDisplayName) FROM final_view WHERE State='VA'"

spark.sql(question_1).coalesce(1).write.format('csv').save("./tmp/output/question_1", header='true')
spark.sql(question_1).printSchema()
spark.sql(question_1).show(truncate=False)

# question_2 = What is the number of LEED projects in Virginia by owner type?
question_2 = "SELECT COUNT(LEEDSystemVersionDisplayName) FROM final_view WHERE State='VA' GROUP BY OwnerTypes"

spark.sql(question_2).coalesce(1).write.format('csv').save("./tmp/output/question_2", header='true')
spark.sql(question_2).printSchema()
spark.sql(question_2).show(truncate=False)

# question_3 = What is the total Gross Square Feet of building space that is LEED-certified in Virginia?
question_3 = "SELECT SUM(GrossSqFoot) AS total_Gross_Square_Feet FROM final_view WHERE IsCertified = 'Yes' AND State='VA'"

spark.sql(question_3).coalesce(1).write.format('csv').save("./tmp/output/question_3", header='true')
spark.sql(question_3).printSchema()
spark.sql(question_3).show(truncate=False)

# question_4 = What Zip Code in Virginia has the highest number of projects?
question_4 = "SELECT Zipcode, COUNT(ProjectName) FROM final_view WHERE State='VA' GROUP BY Zipcode ORDER BY COUNT(ProjectName) DESC"

spark.sql(question_4).coalesce(1).write.format('csv').save("./tmp/output/question_4", header='true')
spark.sql(question_4).printSchema()
spark.sql(question_4).show(truncate=False)

# question_5 = Is there a significant difference (use a t-test) in the points achieved for projects in Virginia compared to California for LEED NC 2.2?
question_5 = "SELECT v.PointsAchieved AS PointsAchieved_in_VA, c.PointsAchieved AS PointsAchieved_in_CA, abs(v.PointsAchieved-c.PointsAchieved) AS significant_difference FROM final_view v INNER JOIN final_view c ON v.ID!=c.ID WHERE c.LEEDSystemVersionDisplayName='LEED-NC 2.2'"

spark.sql(question_5).coalesce(1).write.format('csv').save("./tmp/output/question_5", header='true')
spark.sql(question_5).printSchema()
spark.sql(question_5).show(truncate=False)

spark.stop()
