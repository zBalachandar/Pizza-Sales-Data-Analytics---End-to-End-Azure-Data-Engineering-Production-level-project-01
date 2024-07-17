# Databricks notebook source
dbutils.fs.mount(
  source = "wasbs://raw@sbcblobprodproject001.blob.core.windows.net",
  mount_point = "/mnt/raw",
  extra_configs = {"fs.azure.account.key.sbcblobprodproject001.blob.core.windows.net":"RE/EkfJJuF8pfxk33i5yFMVdxXL2Rw/92xGfhiRMoi8r/FnDED1eLxMHyRqbtplr4TsyMNqac/2x+ASt4wprxA=="})

# COMMAND ----------

ls

# COMMAND ----------

dbutils.fs.ls("/mnt/raw")

# COMMAND ----------

df = spark.read.format("csv").options(header='True',inferSchema='True').load('dbfs:/mnt/raw/dbo.pizza_sales.txt')

# COMMAND ----------

display(df)

# COMMAND ----------

df.createOrReplaceTempView("pizza_sales_analysis")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from pizza_sales_analysis

# COMMAND ----------

# MAGIC %sql
# MAGIC select 
# MAGIC order_id,
# MAGIC quantity,
# MAGIC date_format(order_date,'MMM') month_name,
# MAGIC date_format(order_date,'EEEE') day_name,
# MAGIC hour(order_time) order_time,
# MAGIC unit_price,
# MAGIC total_price,
# MAGIC pizza_size,
# MAGIC pizza_category,
# MAGIC pizza_name
# MAGIC from pizza_sales_Analysis
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select 
# MAGIC count(distinct order_id)order_id,
# MAGIC sum(quantity)quantity,
# MAGIC date_format(order_date,'MMM') month_name,
# MAGIC date_format(order_date,'EEEE') day_name,
# MAGIC hour(order_time) order_time,
# MAGIC sum(unit_price)unit_price,
# MAGIC sum(total_price)total_sales,
# MAGIC pizza_size,
# MAGIC pizza_category,
# MAGIC pizza_name
# MAGIC from pizza_sales_Analysis
# MAGIC group by 3,4,5,8,9,10
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC create table
