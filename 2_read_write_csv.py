# Databricks notebook source
# MAGIC %md
# MAGIC ### we ingest the csv file as a actual csv file,
# MAGIC click on **Data Ingestioin** and **Upload files to a Volume**
# MAGIC
# MAGIC
# MAGIC https://youtu.be/761SQ9Hxbic?list=PLeo1K3hjS3uu7dS3Cx0X796sWUjjBFCuV&t=1298
# MAGIC

# COMMAND ----------

df = spark.read.csv('/Volumes/workspace/default/raw_data/orders.csv', header=True, inferSchema=True)

# COMMAND ----------

df.show()

# COMMAND ----------

df.columns

# COMMAND ----------

df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ###we want to customize the schema type instead of using inferschema=True

# COMMAND ----------

from pyspark.sql import functions as F, types as T
csv_schema = T.StructType([
    T.StructField('order_date', T.DateType()),
    T.StructField('country', T.StringType()),
    T.StructField('order_id', T.IntegerType()),
    T.StructField('product', T.StringType()),
    T.StructField('qty', T.IntegerType()),
    T.StructField('price', T.DoubleType())
])
df = spark.read.csv('/Volumes/workspace/default/raw_data/orders.csv', header=True, schema=csv_schema)
df.show()
df.printSchema()
df_filtered = df.filter((df['imdb_rating'] >= 8.0) & (df['industry'] == 'Animation'))


# COMMAND ----------

# MAGIC %md
# MAGIC ###Dataframe with parquet format

# COMMAND ----------

df.write.mode("overwrite").parquet("/Volumes/workspace/default/raw_data/orders")

# COMMAND ----------



# COMMAND ----------


