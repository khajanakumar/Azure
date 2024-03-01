-- Databricks notebook source
create database if not exists demo

-- COMMAND ----------

show databases

-- COMMAND ----------

use demo


-- COMMAND ----------

DESCRIBE DATABASE EXTENDED demo

-- COMMAND ----------

select current_database()

-- COMMAND ----------



-- COMMAND ----------

-- MAGIC %python
-- MAGIC df =  spark.read.csv("/mnt/formula1dl9/prep/circuits.csv",header=True)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC display(df)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df.write.mode("overwrite").format("parquet").saveAsTable("demo.demo_circuits_parquet")

-- COMMAND ----------

desc extended demo_circuits_parquet

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df.write.mode("overwrite").format("delta").saveAsTable("demo.demo_circuits_delta")

-- COMMAND ----------

desc extended demo_circuits_delta

-- COMMAND ----------

drop table demo_circuits

