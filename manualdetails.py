# Databricks notebook source
#Read file from S3 using databricks - Python
dbutils.widgets.text("file_location", "s3://mytestbktdatabricks/", "Upload Location")
dbutils.widgets.dropdown("file_type", "csv", ["csv", 'parquet', 'json'])
dbutils.widgets.get("file_location")

print(dbutils.widgets.get("file_type"))
print(dbutils.widgets.get("file_location"))
path="s3a://mytestbktdatabricks/details.csv"

df = spark.read.format("csv").option("inferSchema", "true").option("header", "true").load(path)
dbutils.widgets.dropdown("X123", "1", [str(x) for x in range(1, 10)])

dbutils.widgets.dropdown("1", "1", [str(x) for x in range(1, 10)], "hello this is a widget")

dbutils.widgets.dropdown("x123123", "1", [str(x) for x in range(1, 10)], "hello this is a widget")

dbutils.widgets.dropdown("x1232133123", "1", [str(x) for x in range(1, 10)], "hello this is a widget 2")

#print(dbutils.displayHTML(html))
display(df)

display(df.select("address"))

df.createOrReplaceTempView("TEST_PARQUE")



%sql

SELECT age, name,address,batch FROM TEST_PARQUE where age > 13

#Save into 

df.write.format("parquet").mode("overwrite").saveAsTable("MY_PERMANENT_PARQUET_NAME")
display(df)

#df.write.parquet("\tmp", 'data')

df.write.parquet("data/test_table/key=1")

#df1 = spark.read.parquet('/MY_PERMANENT_PARQUET_NAME')

mergedDF = spark.read.option("mergeSchema", "true").parquet("/data/test_table")
mergedDF.printSchema()
#%fs mkdirs file:/tmp/my_local_dir
#dbutils.fs.ls ("file:/MY_PERMANENT_PARQUET*")
#dbutils.fs.put("file:/tmp/my_new_file", "This is a file on the local driver node.")

from delta.tables import DeltaTable

parquet_table ="MY_PERMANENT_PARQUET_NAME" 
partitioning_scheme = "age int"

dlt=DeltaTable.convertToDelta(spark, parquet_table)
print(dlt)
#Converted to Delta table

spark.sql(f"""
DROP TABLE IF EXISTS MY_PERMANENT_PARQUET_NAME
""")

spark.sql(f"""
CREATE TABLE MY_PERMANENT_PARQUET_NAME
USING DELTA
LOCATION "/processed" 
""")


