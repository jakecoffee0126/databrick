# Databricks notebook source
spark

# COMMAND ----------

# MAGIC %md
# MAGIC ### we ingest the csv file as a table instead of csv file itself,
# MAGIC click on **Data Ingestioin** and **Create or modify table**
# MAGIC
# MAGIC https://youtu.be/761SQ9Hxbic?list=PLeo1K3hjS3uu7dS3Cx0X796sWUjjBFCuV&t=375

# COMMAND ----------



# COMMAND ----------

df = spark.table("workspace.default.movies")
df.show()

# COMMAND ----------

display(df)

# COMMAND ----------

df.printSchema()

# COMMAND ----------

df.count()

# COMMAND ----------

df.columns

# COMMAND ----------

df.describe()

# COMMAND ----------

display(df.describe())

# COMMAND ----------

display(df.summary())

# COMMAND ----------

# MAGIC %md
# MAGIC ### Column Filtering

# COMMAND ----------

df_filtered = df.select("title", "imdb_rating", "industry")
df_filtered.show(3, truncate=True) #df_filtered.show(3, truncate=True)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Row Filtering
# MAGIC **using Genie AI** this time, _Ctl+I_, type in 'show me all the movies released between 2000 to 2010'

# COMMAND ----------

df_2000_2010 = df.filter((df.release_year >= 2000) & (df.release_year <= 2010))
display(df_2000_2010)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Second way to filter the movies between 2000 to 2010 by **using col from sql.functions**

# COMMAND ----------

from pyspark.sql.functions import col
df_2000_2010 = df.filter((col("release_year") >= 2000) & (col("release_year") <= 2010))
display(df_2000_2010)

df_2000_2010 = df.filter(col("release_year").between(2000, 2010))
display(df_2000_2010)

# COMMAND ----------

# MAGIC %md
# MAGIC ### more options to filter the movies between 2000 to 2010 by **using col from sql.functions**

# COMMAND ----------


df_2000_2010 = df.filter(df.release_year.between(2000, 2010))
display(df_2000_2010)

df_2000_2010 = df.filter(df.release_year.isin(list(range(2000, 2011))))
display(df_2000_2010)

# COMMAND ----------

# show all the Marvel movies
df_marvel = df.filter(col("studio") == 'Marvel Studios')
display(df_marvel)


# COMMAND ----------

# show how many unique industries are in the dataset
unique_industries = df.select('industry').distinct()
unique_industries.show()

# COMMAND ----------

unique_languages = df.select('language').distinct()
display(unique_languages)

# COMMAND ----------

# show the profit on each movies: revenue - budget
df_profit = df.withColumn('profit', col('revenue') - col('budget'))
display(df_profit)

# COMMAND ----------

df = df.withColumnRenamed('revenue', 'total_revenue')
df.printSchema()

# COMMAND ----------


