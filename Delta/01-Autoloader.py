# Databricks notebook source
# MAGIC %md
# MAGIC # Autoloader to stream data from input container to Bronze layer

# COMMAND ----------

storage_account = "aaysdeadls"
secret_scope = "azure-key-vault-secrets"
secret_name = "de-adls-key"
input_container = "input"
dl_container = "dlake"

spark.conf.set(
    f"fs.azure.account.key.{storage_account}.dfs.core.windows.net",
    dbutils.secrets.get(scope=secret_scope, key=secret_name))

(spark.readStream.format("cloudFiles") 
  .option("cloudFiles.format", "json") 
  .option("cloudFiles.schemaLocation", f"abfss://{dl_container}@{storage_account}.dfs.core.windows.net/") 
  .load(f"abfss://{input_container}@{storage_account}.dfs.core.windows.net/customer/") 
  .writeStream 
  .format("delta")
  .option("mergeSchema", "true") 
  .option("checkpointLocation", f"abfss://{dl_container}@{storage_account}.dfs.core.windows.net/") 
  .start(f"abfss://{dl_container}@{storage_account}.dfs.core.windows.net/bronze/customer/")
)

# COMMAND ----------


