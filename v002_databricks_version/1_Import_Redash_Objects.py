# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC ## Redash Object Importer
# MAGIC ### Step 1: This notebook imports all query, dashboard, and widget objects from a Redash instance and stores to an chosen s3 bucket. 
# MAGIC 
# MAGIC <b> BEFORE YOU START  </b>
# MAGIC 1. Create/use a cluster with write access to the S3 bucket you would like to use to store your Redash Objects. You just need to provide a root bucket/folder path. The migrator will automatically create a subfolder called: /redash_export/
# MAGIC 2. Set environment variables that represent your DBSQL and REDASH API tokens to be used throughout the migration. 
# MAGIC 3. Define your config.json file with the following parmeters: DBSQL_URL, DESTINATION_PREFIX, REDASH_URL, S3_BUCKET_NAME
# MAGIC 4. The following params will be gathered from notebook widgets: EARLIEST_ACTIVE_DATE, TAG_LIST
# MAGIC 
# MAGIC <b> 
# MAGIC   Notes: This notebook allows you to filter objects by tag name modified date time, and if you re-import an already existing object, the object will be overwritten and replaced in S3.

# COMMAND ----------

# DBTITLE 1,Install Redash-Toolbelt dependency
# MAGIC %pip install redash-toolbelt

# COMMAND ----------

import os
import requests
from redash_toolbelt.client import Redash
import json
import datetime

DBX_TOKEN = os.environ.get("DBX_TOKEN")
REDASH_TOKEN = os.environ.get("REDASH_TOKEN")

# COMMAND ----------

# DBTITLE 1,Define Runtime Widgets
dbutils.widgets.text("TAG_LIST (csv)", "", "")
dbutils.widgets.text("EARLIEST_ACTIVE_DATE (yyyy-mm-dd)", "", "")

# COMMAND ----------

# DBTITLE 1,Get Config Params and Widget Params to prepare for load
import json
import os
 
with open(f'{os.getcwd()}/config.json') as conf:
  config = json.load(conf)

print(f"Loading Objects from Redash to s3 with the following parameters: \n")
print(config)

databricks_root_workspace = config.get("DBSQL_URL").strip()
databricks_sql_url = databricks_root_workspace + "/api/2.0/preview/sql/"
redash_root_url = config.get("REDASH_URL").strip()
redash_object_tag_list = [i.strip() for i in dbutils.widgets.get("TAG_LIST (csv)").split()]
redash_object_active_date = dbutils.widgets.get("EARLIEST_ACTIVE_DATE (yyyy-mm-dd)").strip()
bucket_name = config.get("S3_BUCKET_NAME").strip()
dest_prefix = config.get("DESTINATION_PREFIX").strip()

storage_location = f"s3a://{bucket_name}/{dest_prefix}"

print(f"Migrating Redash Objects from {redash_root_url} to : {databricks_root_workspace}")
print(f"Migrating objects with the following tags: {redash_object_tag_list}")
print(f"Migrating objects used after the following date: {redash_object_active_date}")
print(f"Objects stored in s3 buckets: {storage_location}")

# COMMAND ----------

# DBTITLE 1,Functions that Save the Objects to S3
def save_queries(queries_json):

    for query in queries_json:
      
      ### This gets each query, along with its visualizations, and sends each query to json by query Id
      file_name = storage_location + "/redash_export/queries/" + query.get("query_hash") + "_" + query.get("name") + ".json"
      content = json.dumps(query)
      ## This is being done with dbutils
      
      try: 
        dbutils.fs.ls(file_name)
        
        ## If the ls is successful, there is a file there, so remove and replace
        dbutils.fs.rm(file_name, recurse=True)
        
        ## Insert New Query Object
        dbutils.fs.put(file_name, contents=content)
        
      except: 
        dbutils.fs.put(file_name, contents=content)


def save_dashboards(dashboards_json):
    ## Create a named separate file for each object for easy organization 
    
    for dashboard in dashboards_json:
      ### This gets each query, along with its visualizations, and sends each query to json by query Id
      file_name = storage_location + "/redash_export/dashboards/" + dashboard.get("slug") + "_" + dashboard.get("name") + ".json"
      content = json.dumps(dashboard)
      ## This is being done with dbutils
      try: 
        dbutils.fs.ls(file_name)
        ## If the ls is successful, there is a file there, so remove and replace
        dbutils.fs.rm(file_name, recurse=True)

        ## Insert new Dashboard object
        dbutils.fs.put(file_name, contents=content)

      except: 
        dbutils.fs.put(file_name, contents=content)


# COMMAND ----------

# DBTITLE 1,Initialize Redash Client
redash = Redash(redash_root_url, REDASH_TOKEN)

# COMMAND ----------

# DBTITLE 1,Get Redash Queries with desired filters
### Filter initially by tag list
if len(redash_object_tag_list) >=1:
    ## If query matches any of the tags need to do this 100k times
    filtered_tag_queries = [ i for i in redash.queries(1,5)["results"] if len(set(i.get("tags")).intersection(redash_object_tag_list)) > 0]
    
else:
    filtered_tag_queries = redash.paginate(redash.queries(2,1), 1, 1)

### Filter by modified date

if len(redash_object_active_date) >= 1:
  desired_active_date_filter = datetime.datetime.strptime(redash_object_active_date,"%Y-%m-%d")
  filtered_modified_date_queries = [i for i in filtered_tag_queries if datetime.datetime.strptime(i.get("updated_at")[0:19],"%Y-%m-%dT%H:%M:%S") >= desired_active_date_filter]
  
else: 
  filtered_modified_date_queries = filtered_tag_queries

  
print(f"Loading {len(filtered_modified_date_queries)} queries to S3 with filters: \n Tags :{redash_object_tag_list} \n Modified Date: {desired_active_date_filter}")

# COMMAND ----------

# DBTITLE 1,Save Selected Queries to s3
save_queries(filtered_modified_date_queries)

# COMMAND ----------

# DBTITLE 1,Get Dashboards and Save to S3
### Filter initially by tag list
if len(redash_object_tag_list) >=1:
    ## If query matches any of the tags need to do this 100k times
    filtered_tag_dashboards = [ i for i in redash.dashboards(1,5)["results"] if len(set(i.get("tags")).intersection(redash_object_tag_list)) > 0]
    
else:
    filtered_tag_dashbaords = redash.paginate(redash.dashboards(2,1), 1, 1)

### Filter by modified date

if len(redash_object_active_date) >= 1:
  desired_active_date_filter = datetime.datetime.strptime(redash_object_active_date,"%Y-%m-%d")
  filtered_modified_date_dashboards = [i for i in filtered_tag_dashboards if datetime.datetime.strptime(i.get("updated_at")[0:19],"%Y-%m-%dT%H:%M:%S") >= desired_active_date_filter]
  
else: 
  filtered_modified_date_dashboards = filtered_tag_dashboards

  
print(f"Loading {len(filtered_modified_date_dashboards)} dashbaords to S3 with filters: \n Tags :{redash_object_tag_list} \n Modified Date: {desired_active_date_filter}")

# COMMAND ----------

print(filtered_modified_date_dashboards[0])

# COMMAND ----------

# DBTITLE 1,Save Dashboards
save_dashboards(filtered_modified_date_dashboards)
