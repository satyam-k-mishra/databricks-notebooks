# Databricks notebook source
dbutils.widgets.text("container_name", "")
dbutils.widgets.text("account_name", "capstone01")
container_name = dbutils.widgets.get("container_name")
account_name = dbutils.widgets.get("account_name")

# COMMAND ----------

# Databricks secrets to connect to ADLS
applicationId = dbutils.secrets.get(scope="capstone01", key="appid")
authenticationKey = dbutils.secrets.get(scope="capstone01", key="clientsecret")
tenantId = dbutils.secrets.get(scope="capstone01", key="tenantid")

# COMMAND ----------

# Connection Strings for adls container
adlsAccountName = account_name
adlsContainerName = container_name
mountPoint = f"/mnt/files/{container_name}"
source = f"abfss://{adlsContainerName}@{adlsAccountName}.dfs.core.windows.net/"

# COMMAND ----------

# Directory (Tanant) ID for authentication
endpoint = f"https://login.microsoftonline.com/{tenantId}/oauth2/token"

# COMMAND ----------

# Service principal secrets and OAuth configurations
configs = {
  "fs.azure.account.auth.type": "OAuth",
  "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
  "fs.azure.account.oauth2.client.id": applicationId,
  "fs.azure.account.oauth2.client.secret": authenticationKey,
  "fs.azure.account.oauth2.client.endpoint": endpoint,
}

# COMMAND ----------

# Mount ADLS Stroage to DBFS
# Mount only if the directory is not already mounted otherwise it will give error
if not any(mount.mountPoint == mountPoint for mount in dbutils.fs.mounts()):
  dbutils.fs.mount(source=source, mount_point=mountPoint, extra_configs=configs)
