from pyspark.sql import SparkSession
from pyspark.sql.functions import mean, round, max

# Create a Spark session
spark = SparkSession.builder.appName("practice1").getOrCreate()
file_path = '../data_files/data1.csv'
# Assuming data is read into a Spark DataFrame called df
df = spark.read.csv(file_path, header=True, inferSchema=True)

# 1. Print df.head
df.show(5)

# 2. Get max funding_amount
max_funding = df.agg(max("funding_amount_usd")).collect()[0][0]
print("\nMax funding amount:", max_funding)

# 3. Get avg funding_amount per company_name (rounded to whole number)
avg_funding_per_company = (df.groupBy("company_name")
                           .agg(round(mean("funding_amount_usd")).alias("avg_funding"))
                           .orderBy("avg_funding", ascending=False))
print("\nAverage funding amount per company:")
avg_funding_per_company.show()

# 4. Get highest avg funding_amount per company_name where country = "United States"
us_companies = df.filter(df["region"] == "United States")
highest_avg_funding_us = (us_companies.groupBy("company_name")
                          .agg(round(mean("funding_amount_usd")).alias("avg_funding"))
                          .agg(max("avg_funding"))
                          .collect()[0][0])
print("\nHighest average funding amount for a company in the United States:", highest_avg_funding_us)

# Stop the Spark session
spark.stop()