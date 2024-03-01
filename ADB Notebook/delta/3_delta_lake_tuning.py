# Databricks notebook source
# MAGIC %md
# MAGIC 1. Write data to delta lake (managed table)
# MAGIC 2. Write data to delta lake (external table)
# MAGIC 3. Read data from delta lake (Table)
# MAGIC 4. Read data from delta lake (File)
# MAGIC 5. Delta lake Tuning commands like optimixze,Zorder etc
# MAGIC 6. Refernce https://learn.microsoft.com/en-us/azure/databricks/delta/tune-file-size

# COMMAND ----------

# MAGIC %sql
# MAGIC drop database if exists f1_delta
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS f1_delta
# MAGIC LOCATION '/mnt/formula1dl9/delta'

# COMMAND ----------

circuits_df = spark.read \
.option("inferSchema", True) \
.csv("/mnt/formula1dl9/prep/circuits.csv",header = True)

# COMMAND ----------

display(circuits_df)

# COMMAND ----------

circuits_df.write.format("delta").mode("overwrite").saveAsTable("f1_delta.circuits1")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_delta.circuits1;

# COMMAND ----------

# MAGIC %sql
# MAGIC delete from f1_delta.circuits1 where circuitid between 71 and 77

# COMMAND ----------

# MAGIC %sql
# MAGIC update f1_delta.circuits1
# MAGIC set url = 'N/A1'
# MAGIC where circuitid between 1 and 10

# COMMAND ----------

# MAGIC %sql
# MAGIC DESC HISTORY f1_delta.circuits1

# COMMAND ----------

races_df = spark.read \
.option("inferSchema", True) \
.csv("/mnt/formula1dl9/prep/races.csv",header=True)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_delta.circuits1 version as of 1

# COMMAND ----------

# MAGIC %sql
# MAGIC optimize f1_delta.circuits1

# COMMAND ----------

# MAGIC %sql
# MAGIC SET spark.databricks.delta.retentionDurationCheck.enabled = false;
# MAGIC VACUUM f1_delta.circuits1 RETAIN 3 HOURS 

# COMMAND ----------

display(races_df)

# COMMAND ----------

races_df.write.format("delta").mode('overwrite').partitionBy('year').save('/mnt/formula1dl9/delta/races')

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE f1_delta.races_external
# MAGIC USING DELTA
# MAGIC LOCATION '/mnt/formula1dl9/delta/races'

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_delta.races_external

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW PARTITIONS f1_delta.races_external

# COMMAND ----------

# MAGIC %md
# MAGIC 1. Update Delta Table
# MAGIC 2. Delete From Delta Table

# COMMAND ----------

# MAGIC %sql
# MAGIC delete from f1_delta.races_external where year <=  2000

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_delta.races_external where year = 2021

# COMMAND ----------

# MAGIC %sql
# MAGIC update f1_delta.races_external
# MAGIC set url =  'N/A'
# MAGIC where year = 2021

# COMMAND ----------

# MAGIC %sql
# MAGIC describe table extended f1_delta.races_external 

# COMMAND ----------

# MAGIC %sql DESC HISTORY f1_delta.races_external

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace view  races_tempview as select raceid,year from f1_delta.races_external

# COMMAND ----------

# MAGIC %sql
# MAGIC  CREATE or REPLACE TEMPORARY VIEW races_global_tempview as select raceid,year from f1_delta.races_external

# COMMAND ----------

# MAGIC %sql
# MAGIC  CREATE or REPLACE  VIEW races_global_tempview as select * from f1_delta.races_external

# COMMAND ----------

# MAGIC %md
# MAGIC Day 2

# COMMAND ----------

# MAGIC %md
# MAGIC 1. History & Versioning
# MAGIC 2. Time Travel
# MAGIC 3. Vaccum

# COMMAND ----------

# MAGIC %sql
# MAGIC DESC HISTORY f1_delta.races_external

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_delta.races_external VERSION AS OF 2;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_delta.races_external TIMESTAMP AS OF '2024-02-20T19:10:28Z' where year <= 2000;

# COMMAND ----------

df = spark.read.format("delta").option("timestampAsOf", '2024-02-20T19:10:28Z').load("/mnt/formula1dl9/delta/races")

# COMMAND ----------

display(df)

# COMMAND ----------

# MAGIC %sql
# MAGIC VACUUM f1_delta.races_external

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_delta.races_external TIMESTAMP AS OF '2024-02-20T19:10:28Z';

# COMMAND ----------

# MAGIC %sql
# MAGIC SET spark.databricks.delta.retentionDurationCheck.enabled = false;
# MAGIC VACUUM f1_delta.races_external RETAIN 0 HOURS

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_delta.races_external TIMESTAMP AS OF '2024-02-20T19:10:28Z';

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_delta.races_external

# COMMAND ----------

# MAGIC %sql
# MAGIC DESC HISTORY f1_delta.races_external

# COMMAND ----------

# MAGIC %md
# MAGIC Convert Parquet to Delta

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS f1_delta.races_convert_to_delta (
# MAGIC raceid INT,
# MAGIC yearid INT,
# MAGIC roundid INT
# MAGIC )
# MAGIC USING PARQUET
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO f1_delta.races_convert_to_delta (raceid,yearid,roundid)
# MAGIC SELECT raceid,year,round FROM f1_delta.races_external

# COMMAND ----------

# MAGIC %sql
# MAGIC CONVERT TO DELTA f1_delta.races_convert_to_delta

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table f1_delta.races_convert_to_delta

# COMMAND ----------

circuits_df.write.format("parquet").mode("overwrite").save("/mnt/formula1dl9/delta/circuit_convert_to_delta")

# COMMAND ----------

# MAGIC %sql
# MAGIC CONVERT TO DELTA parquet.`/mnt/formula1dl9/delta/circuit_convert_to_delta`

# COMMAND ----------

display(spark.read.format("delta").load("/mnt/formula1dl9/delta/circuit_convert_to_delta"))

# COMMAND ----------

display_delta_df = spark.read.format("delta").load("/mnt/formula1dl9/delta/circuit_convert_to_delta")

# COMMAND ----------

display_delta_df.printSchema()

# COMMAND ----------


