# Databricks notebook source
# MAGIC %md
# MAGIC ### Mount Azure Data Lake using Service Principal
# MAGIC #### Steps to follow
# MAGIC 1. Get client_id, tenant_id and client_secret from key vault
# MAGIC 2. Set Spark Config with App/ Client Id, Directory/ Tenant Id & Secret
# MAGIC 3. Call file system utlity mount to mount the storage
# MAGIC 4. Explore other file system utlities related to mount (list all mounts, unmount)

# COMMAND ----------

client_id = dbutils.secrets.get(scope = 'formula1-scope', key = 'formula1dl9-app-clientid')
tenant_id = dbutils.secrets.get(scope = 'formula1-scope', key = 'formula1dl9-app-tenant-id')
client_secret = dbutils.secrets.get(scope = 'formula1-scope', key = 'formula1dl9-app-client-secret')

# COMMAND ----------

configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": client_id,
          "fs.azure.account.oauth2.client.secret": client_secret,
          "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}

# COMMAND ----------

dbutils.fs.mount(
  source = "abfss://dp203prep@formula1dl9.dfs.core.windows.net/",
  mount_point = "/mnt/formula1dl9/dp203prep",
  extra_configs = configs)

# COMMAND ----------

dbutils.fs.mount(
  source = "abfss://dp203prep@formula1dl9.dfs.core.windows.net/",
  mount_point = "/mnt/formula1dl9/dp203delta",
  extra_configs = configs)


# COMMAND ----------

display(dbutils.fs.ls("/mnt/formula1dl9/dp203prep"))

# COMMAND ----------

df =  spark.read.csv("/mnt/formula1dl9/dp203prep/circuits.csv",header=True)
df.printSchema()
df.count()
df.show(5,truncate = False)

# COMMAND ----------

df.show(5,truncate = False)

# COMMAND ----------

df.describe(["lat"]).show()

# COMMAND ----------

column_to_remove = ["url"]
df_new = df.select([column for column in df.columns if column not in column_to_remove])
display(df_new)

# COMMAND ----------

import pyspark.sql.functions as f

df_new_expand = df_new.withColumn('country_code',f.lit('USA'))
display(df_new_expand)

# COMMAND ----------

df_new_expand.createOrReplaceTempView("circuit_view")



# COMMAND ----------

spark.sql("select * from circuit_view")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from circuit_view

# COMMAND ----------

display(spark.read.csv("/mnt/formula1dl9/dp203prep/circuits.csv",header=True))

# COMMAND ----------

display(dbutils.fs.mounts())

# COMMAND ----------

dbutils.fs.unmount('/mnt/formula1dl9/dp203prep')

# COMMAND ----------


