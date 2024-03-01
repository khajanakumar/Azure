# Databricks notebook source
# MAGIC %md
# MAGIC - Shell Utities
# MAGIC - Magic Commands
# MAGIC - OS Commands

# COMMAND ----------

# MAGIC %sh ps

# COMMAND ----------

# MAGIC %fs ls

# COMMAND ----------

dbutils.fs.ls('/')

# COMMAND ----------

print ( dbutils.fs.ls('/')[0])

# COMMAND ----------

for file in dbutils.fs.ls('/'):
    print(file)

# COMMAND ----------

for file in dbutils.fs.ls('/'):
    if file.name.endswith('/'):
        print(file)
    

# COMMAND ----------

dbutils.fs.ls('dbfs:/databricks-datasets/')

# COMMAND ----------

dbutils.help()

# COMMAND ----------

dbutils.fs.help('ls') 

# COMMAND ----------


