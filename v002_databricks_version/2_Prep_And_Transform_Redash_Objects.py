# Databricks notebook source
# MAGIC %md
# MAGIC ## Redash Object Transformer
# MAGIC ### Step 2: This notebook transforms and applies mappings passed trough to replace table and function names to allow for easier migration from Radash to DBSQL
# MAGIC 
# MAGIC <b> BEFORE YOU START  </b>
# MAGIC 1. Create/use a cluster with write access to the S3 bucket you would like to use to store your Redash Objects. You just need to provide a root bucket/folder path. The migrator will automatically create a subfolder called: /redash_export/
# MAGIC 2. Set environment variables that represent your DBSQL and REDASH API tokens to be used throughout the migration. 
# MAGIC 
# MAGIC ### Cluster Config Mappings Templates/Examples: 
# MAGIC 
# MAGIC {
# MAGIC "table_mappings":{"parquet_parquet":"codydemoes.c_event","oldtable":"newtable"},
# MAGIC "function_mappings":{"strpos":"instr","from_iso8601_timestamp":"date"}
# MAGIC }
# MAGIC 
# MAGIC 
# MAGIC <b> 2 Top Level Attributes: table_mappings ({oldtable:new_table_name_fully_qualified}) and function_mappings ({old_function:new_function}) </b>
# MAGIC 
# MAGIC Notes:
# MAGIC 
# MAGIC <li> 1. New table names must be fully qualified (database.table) names registered in the DBSQL Hive Metastore
# MAGIC <li> 2. All Data Sources are now a DBSQL Cluster and Hive Metastore, so this tool assumes all external tables have been migrated to Databricks

# COMMAND ----------

import re
import json
import os
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

with open(f"{os.getcwd()}/config.json") as conf:
  config = json.load(conf)
  
print("Cleaning and Saving Queries to Delta to Prepare for DBSQL load with the following params: \n")
print(config)

# COMMAND ----------

bucket_name = config.get("S3_BUCKET_NAME")
file_path = f"{os.getcwd()}/mappings/"
dest_prefix = config.get("DESTINATION_PREFIX")

# COMMAND ----------

# DBTITLE 1,Get Mappings

## Load table mappings
try: 
  with open(f"{os.getcwd()}/mappings/tablemappings.json") as table_map:
     table_mappings = json.load(table_map)
      
  print(f"Table Mappings: \n {table_mappings} \n")
  
except: 
  table_mappings = None
  print("No table mappings defined, loading objects as-is")
  
## Load Function Mappings: 
try: 
  with open(f"{os.getcwd()}/mappings/functionmappings.json") as function_map:
    function_mappings = json.load(function_map)
    
  print(f"Function Mappings: \n {function_mappings} \n")
  
except: 
  function_mappings = None
  print("No function mappings defined, loading objects as-is")


# COMMAND ----------

##### Perform REGEX replace statements on all query definitions to replace old table names with new table names
import json
import re

def multiple_replace(string, rep_dict):
    pattern = re.compile("|".join([re.escape(k.lower()) for k in sorted(rep_dict,key=len,reverse=True)]), flags=re.DOTALL)
    return pattern.sub(lambda x: rep_dict[x.group(0)], string.lower())

# COMMAND ----------

@udf()
def clean_and_write_queries(inputCol):

  query = json.loads(str(inputCol))

  if len(dest_prefix) >= 1:
    storage_location = f"s3://{bucket_name}/{dest_prefix}"
  else:
    storage_location = f"s3://{bucket_name}"

  source_string = query.get("query")
  query_name = query.get("name")
  new_query = None
  
  try: 
    if table_mappings is not None:
      new_query = multiple_replace(source_string, table_mappings)
      print(f"Completed table mappings for query: {query_name}")

    if function_mappings is not None:
      new_query = multiple_replace(new_query, function_mappings)
      print(f"Completed function mappings for query: {query_name}")

    if new_query is not None:
      query["query"] = new_query

  except Exception as e:
    print(f"Failed to parse query... {str(e)}")
    ## else dont replace the query
    
  ### This gets each query, along with its visualizations, and sends each query to json by query Id
  file_name = f"{dest_prefix}/redash_export/cleaned_queries/" + query.get("query_hash") + "_" + query.get("name") + ".json"
  content = json.dumps(query)
  """
  result = s3client.put_object(
    Body=content,
    Bucket=bucket_name,
    Key=file_name
    )
  """
  return content

# COMMAND ----------

spark.sql("CREATE DATABASE IF NOT EXISTS redash_migration")
spark.sql("USE redash_migration;")

# COMMAND ----------

spark.sql(f"""

CREATE TABLE IF NOT EXISTS redash_migration.cleaned_queries
(query_name STRING,
query_hash STRING,
tags ARRAY<STRING>,
query_object STRING,
update_date_time STRING,
is_migrated BOOLEAN,
upload_message STRING
)
USING DELTA 
LOCATION 's3://{bucket_name}/{dest_prefix}/redash_export/cleaned_queries/'
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS redash_migration.cleaned_dashboards
(dashboard_name STRING,
dashboard_slug STRING,
tags ARRAY<STRING>,
dashboard_object STRING,
update_date_time STRING,
is_migrated BOOLEAN,
upload_message STRING
)
USING DELTA 
LOCATION 's3://{bucket_name}/{dest_prefix}/redash_export/cleaned_dashboards/'
""")

# COMMAND ----------

df = spark.read.format("text").load(f"s3://{bucket_name}/{dest_prefix}/redash_export/queries/")


cleaned_queries = (df.withColumnRenamed("value", "OriginalQuery").withColumn("CleanedQuery", clean_and_write_queries("OriginalQuery"))
                .withColumn("update_date_time", current_timestamp())
                 .selectExpr("CleanedQuery:name AS query_name", 
                             "CleanedQuery:query_hash AS query_hash",
                             "CleanedQuery:tags AS tags",
                             "CleanedQuery AS query_object",
                             "update_date_time",
                             "False AS is_migrated")
                )


cleaned_queries.withColumn("tags", split(regexp_replace(col("tags"), "[^A-Za-z,_]", ""), ",")).createOrReplaceTempView("clean_queries_temp")

# COMMAND ----------

# DBTITLE 1,Merge Query Objects - Replace If Already Exist with Newest Pull
spark.sql("""MERGE INTO redash_migration.cleaned_queries AS target
USING clean_queries_temp AS source
ON source.query_hash = target.query_hash AND source.query_name = target.query_name
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
""")

# COMMAND ----------

# DBTITLE 1,Save Dashboards into Delta
df_dash = spark.read.format("text").load(f"s3://{bucket_name}/{dest_prefix}/redash_export/dashboards/")


cleaned_dashboards = (df_dash.withColumnRenamed("value", "OriginalDashboard")
                .withColumn("update_date_time", current_timestamp())
                 .selectExpr("OriginalDashboard:name AS dashboard_name", 
                             "OriginalDashboard:slug AS dashboard_slug",
                             "OriginalDashboard:tags AS tags",
                             "OriginalDashboard AS dashboard_object",
                             "update_date_time",
                            "False AS is_migrated")
                 .withColumn("tags", split(regexp_replace(col("tags"), "[^A-Za-z,_]", ""), ","))
                )


cleaned_dashboards.createOrReplaceTempView("clean_dashboards_temp")

# COMMAND ----------

spark.sql("""MERGE INTO redash_migration.cleaned_dashboards AS target
USING clean_dashboards_temp AS source
ON source.dashboard_slug = target.dashboard_slug AND source.dashboard_name = target.dashboard_name
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
""")

# COMMAND ----------

# DBTITLE 1,Show Queries to Migrate
# MAGIC %sql
# MAGIC SELECT * FROM redash_migration.cleaned_queries

# COMMAND ----------

# DBTITLE 1,Show Dashboards to Migrate
# MAGIC %sql
# MAGIC 
# MAGIC SELECT * FROM redash_migration.cleaned_dashboards
