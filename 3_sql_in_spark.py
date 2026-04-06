# Databricks notebook source


# COMMAND ----------

# MAGIC %md
# MAGIC ## SQL Using Python

# COMMAND ----------

df_marvel = spark.sql("select * from workspace.default.movies where studio LIKE '%Marvel%'")
#df_marvel = spark.sql("select * from workspace.default.movies where studio='Marvel Studios'")
display(df_marvel)

# COMMAND ----------

# MAGIC %md
# MAGIC ## SQL Using %sql

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from workspace.default.movies where studio='Marvel Studios'

# COMMAND ----------

# MAGIC %md
# MAGIC ## Creating a temporary dataframe 
# MAGIC ### Create a Dataframe Using List

# COMMAND ----------

from pyspark.sql import functions as F, types as T

data = [
    ("2017-01-01", 32.0,  6.0,  "Rain"),
    ("2017-01-04", None,  9.0,  "Sunny"),
    ("2017-01-05", 28.0,  None, "Snow"),
    ("2017-01-06", None,  7.0,  None),
    ("2017-01-07", 32.0,  None, "Rain"),
    ("2017-01-08", None,  None, "Sunny"),
    ("2017-01-09", None,  None, None),
    ("2017-01-10", 34.1,  8.1,  "Cloudy"),
    ("2017-01-11", 40.0, 12.0,  "Sunny"),
]

schema = "day string, temperature double, windspeed double, event string"
df = spark.createDataFrame(data, schema)

# if there is a column name is 'day', it will just replace it
df = df.withColumn("day", F.to_date("day", "yyyy-MM-dd"))  # normalize to DateType

#if there is no column name 'day-new', it will treat 'day-new' as a new column, and added it to the end
#of the table 
df_new_day = df.withColumn("day-new", F.to_date("day", "yyyy-MM-dd"))  

display(df)
display(df_new_day)

# COMMAND ----------

df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Creating or replacing 'Weather' with SQL

# COMMAND ----------

df.createOrReplaceTempView("weather")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT event, ROUND(AVG(temperature), 1) AS avg_temp
# MAGIC FROM weather
# MAGIC GROUP BY event
# MAGIC ORDER BY avg_temp DESC;

# COMMAND ----------

# df.createOrReplaceTempView("weather") # this is session scoped
# df.createGlobalTempView("global_weather")# this is global scoped


# COMMAND ----------

# MAGIC %md
# MAGIC ## Performed join in Spark

# COMMAND ----------

from pyspark.sql import functions as F, types as T  

rows_customers = [
    (1,  "Asha",  "IN", True),
    (2,  "Bob",   "US", False),
    (3,  "Chen",  "CN", True),
    (4,  "Diana", "US", None),
    (None, "Ghost","UK", False),     # NULL key to demo null join behavior
]

rows_orders = [
    (101, 1,   120.0, "IN"),
    (102, 1,    80.0, "IN"),
    (103, 2,    50.0, "US"),
    (104, 5,    30.0, "DE"),         # no matching customer_id
    (105, 3,   200.0, "CN"),
    (106, None, 15.0, "UK"),         # NULL key won’t match
    (107, 3,    40.0, "CN"),
    (108, 2,    75.0, "US"),
]

#specify the schema
schema_customers = T.StructType([
    T.StructField("customer_id", T.IntegerType(), True),
    T.StructField("name",        T.StringType(),  True),
    T.StructField("country",     T.StringType(),  True),
    T.StructField("vip",         T.BooleanType(), True),
])

schema_orders = T.StructType([
    T.StructField("order_id",    T.IntegerType(), True),
    T.StructField("customer_id", T.IntegerType(), True),
    T.StructField("amount",      T.DoubleType(),  True),
    T.StructField("country",     T.StringType(),  True),  # same column name to show collisions
])

df_customers = spark.createDataFrame(rows_customers, schema_customers)
df_orders = spark.createDataFrame(rows_orders, schema_orders)

display(df_customers)
display(df_orders)



# COMMAND ----------

df_inner_join = df_orders.join(df_customers, on="customer_id", how="inner")
display(df_inner_join)

# COMMAND ----------

df_left_join = df_orders.join(df_customers, on="customer_id", how="left")
#df_left_join = df_orders.join(df_customers, "customer_id", "left") # you can also write like this
display(df_left_join)

# COMMAND ----------

#alias
o = df_orders.alias("o")
c = df_customers.alias("c")
# o,c = df_orders.alias("o"),df_customers.alias("c")

df_inner_join = o.join(c, o.customer_id == c.customer_id, "inner")
display(df_inner_join)
#

# COMMAND ----------

#The table above has two countries, so we want to specify it
df_inner_clean = (
    o.join(c, on="customer_id", how="inner")
    .select("order_id", "customer_id", "amount", 
            F.col("o.country").alias("ship_country"), 
            "name", 
            F.col("c.country").alias("customer_country"), 
            "vip")
)

display(df_inner_clean)

# COMMAND ----------

#full join

df_full_join = df_orders.join(df_customers, on="customer_id", how="full")
display(df_full_join)

# COMMAND ----------

# left semi join
# https://youtu.be/761SQ9Hxbic?list=PLeo1K3hjS3uu7dS3Cx0X796sWUjjBFCuV&t=2872

df_leftsemi_join = df_orders.join(df_customers, on="customer_id", how="left_semi") #orders with a known customer
display(df_leftsemi_join)

# COMMAND ----------

# left anti join - its the oopposite of left semi join
# hhttps://youtu.be/761SQ9Hxbic?list=PLeo1K3hjS3uu7dS3Cx0X796sWUjjBFCuV&t=2918

df_leftanti_join = df_orders.join(df_customers, on="customer_id", how="left_anti") #orpah order (no matching customer)
display(df_leftanti_join)

# COMMAND ----------

# join multiple columns
# https://youtu.be/761SQ9Hxbic?list=PLeo1K3hjS3uu7dS3Cx0X796sWUjjBFCuV&t=2949
df_multi = df_orders.join(df_customers, on=["customer_id", "country"], how="inner") #orpah order (no matching customer)
display(df_multi)

# COMMAND ----------


