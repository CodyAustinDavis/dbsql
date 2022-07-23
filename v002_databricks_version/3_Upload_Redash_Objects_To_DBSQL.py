# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC ## Author: Cody Austin Davis
# MAGIC ## Creation Date: 11.1.2021
# MAGIC ### Use Case: Migration for all query and dashboard objects from a self managed redash intances into a DatabricksSQL managed enviornment. 
# MAGIC 
# MAGIC #### Steps:
# MAGIC 1. Gets data source Id to insert into 
# MAGIC     <li> Uploads query objects 
# MAGIC         <li> Add visualizations 
# MAGIC             <li> Uploads dashboard object shells 
# MAGIC                 <li> Add widgets
# MAGIC                   
# MAGIC                   
# MAGIC #### Functionality Notes: 
# MAGIC <li> module can be imported and accessed ad hoc for more interactively add and remove/change objects
# MAGIC <li> Can change the run mode if running main method
# MAGIC   
# MAGIC #### Assumptions: 
# MAGIC <li> All endpoint names are unique
# MAGIC <li> All data_source_ids from the self hosted redash will be converged into a single endpoint to ensure queries work
# MAGIC <li> Query objects must be created for visualziations to exist, and dashboard shells must exists before widgets can be inserted into them
# MAGIC <li> Visualizations Ids are global across queries, I need to test this assumption!!
# MAGIC   
# MAGIC #### To Do:
# MAGIC <li> Check for duplicate query names, and handle by either overwriting or creating new query name - v2
# MAGIC <li> Handle the fully qualified table naming changes - i.e. map all table names to a database and rename queries (stretch goal)
# MAGIC <li> Handle layout data
# MAGIC <li> Handle uploading of whole dashboards with associated visualizations and queries automatically

# COMMAND ----------

# DBTITLE 1,Imports / ENV Varibles
import os
import requests
import json
import datetime
from pyspark.sql.functions import *
from pyspark.sql.types import *

DBX_TOKEN = os.environ.get("DBX_TOKEN")

# COMMAND ----------

# DBTITLE 1,Define Runtime Widgets
dbutils.widgets.text("TAG_LIST (csv)", "", "")
dbutils.widgets.dropdown("Insert Mode", "Tags", ["Tags", "Query Name", "Both (and)", "Both (or)", "ALL"])
dbutils.widgets.text("DBSQL Endpoint Name To Use", "")

# COMMAND ----------

# DBTITLE 1,Load Configs
with open(f"{os.getcwd()}/config.json") as conf:
  config = json.load(conf)
  
print(config)

databricks_root_workspace = config.get("DBSQL_URL").strip()
databricks_sql_url = databricks_root_workspace + "/api/2.0/preview/sql/"
redash_object_tag_list = [i.strip() for i in dbutils.widgets.get("TAG_LIST (csv)").split()]
bucket_name = config.get("S3_BUCKET_NAME").strip()
dest_prefix = config.get("DESTINATION_PREFIX").strip()
databricks_sql_endpoint_name = dbutils.widgets.get("DBSQL Endpoint Name To Use").strip()

storage_location = f"s3a://{bucket_name}/{dest_prefix}"

print(f"Migrating Redash Objects to : {databricks_root_workspace}")
print(f"Migrating objects with the following tags: {redash_object_tag_list}")
#print(f"Migrating objects used after the following date: {redash_object_active_date}")
print(f"Objects stored in s3 buckets: {storage_location}")
print(f"Migrating With Endpoint : {databricks_sql_endpoint_name}")

# COMMAND ----------

# DBTITLE 1,Use Isolated Database
spark.sql("USE redash_migration")

# COMMAND ----------

# DBTITLE 1,Get Tables
query_objects = spark.table("redash_migration.cleaned_queries")
dashboard_objects = spark.table("redash_migration.cleaned_dashboards")

# COMMAND ----------

class ConfigException(Exception):
    def __init__(self, *args):
        if args:
            self.message = args[0]
        else:
            self.message = None

    def __str__(self):
        if self.message:
            return f'Config Error: {self.message}'
        else:
            return f'Config Error: Something is not configured correctly'

# COMMAND ----------

# DBTITLE 1,Get Data Source Id of Selected Endpoint
uri = databricks_sql_url+ "data_sources"
##### Get the Data source Id and endpoint id of the cluster you are going to assign all these queries to at first (data_source_id will soon be decoupled from queries)
## This API endpoint is not currently documented publicly
headers_auth = {"Authorization":f"Bearer {DBX_TOKEN}"}

## This file could be large
endp_resp = requests.get(uri, headers=headers_auth).json()

# COMMAND ----------

# DBTITLE 1,Select Endpoint with Specific Data Source Id
try: 

  dbsql_data_source_id = None
  dbsql_endpoint_id_to_use = None

  if len(databricks_sql_endpoint_name) >= 1: 

      ### If multiple matches, just pick one
      dbsql_endpoint_id_to_use = str([endpoint.get("endpoint_id") for endpoint in endp_resp if endpoint.get("name") == databricks_sql_endpoint_name][0])
      dbsql_data_source_id = str([endpoint.get("id") for endpoint in endp_resp if endpoint.get("name") == databricks_sql_endpoint_name][0])

      print(f"Running Migration from Redash to DBSQL on endpoint: {databricks_sql_endpoint_name} with data_source_id = {dbsql_data_source_id} and endpoint_id = {dbsql_endpoint_id_to_use}")
  else: 
      raise ConfigException('Please specify a valid endpoint name to migrate to!') 
      
except Exception as e:
  resp = str(endp_resp.get("error_code"))
  print(f"Failed to connect with error code {resp}")

# COMMAND ----------

# DBTITLE 1,Functions to upload Query object in a Spark udf in parallel
@udf("string")
def upload_queries(raw_query, dbsql_endpoint_id, dbsql_url):
      import requests
      import json
      
      headers = {"Authorization":f"Bearer {DBX_TOKEN}"}
      dbsql_endpoint_id = str(dbsql_endpoint_id)
      dbsql_url = str(dbsql_url)
      active_query = json.loads(raw_query)
      visualization_id_mappings = []
      
      return_objs = {}
      
      final_resp = []
      new_query = {}
      q_name = active_query.get("name")

      ## Get Mapped data source Id of where you want the query to point to in DBSQL (assumes that data source already exists, cant have a query without a Data source)

      redash_data_source_id = active_query.get("data_source_id")
      #print(f"Redash Data Source Id {redash_data_source_id}")

      print(f"Pushing Redash Query: {q_name} from Redash Data Source Id: {redash_data_source_id} to DataSQL Data Source Id: {dbsql_data_source_id}... (endpoint Id of : {dbsql_endpoint_id})")

      ## global variable, sloppy I know
      new_query["data_source_id"] = dbsql_data_source_id
      new_query["query"] = active_query.get("query")
      new_query["name"] = active_query.get("name")
      new_query["description"] = active_query.get("description") or ""
      new_query["schedule"] = active_query.get("schedule")
      new_query["options"] = active_query.get("options")
      new_query["visualizations"] = active_query.get("visualizations")
      new_query["tags"] = ["migrated_from_redash"].append(active_query.get("tags")) or ["migrated_from_redash"]          

      ## Submit Query to API in DBSQL, must create the query, get the Id, and then alter the query with its Id to add everything else (visuals)
      new_query_json = json.dumps(new_query)

      #print(new_query_json)

      try:
          resp = requests.post(f"{dbsql_url}queries", data=new_query_json, headers=headers)

          if resp.status_code == 200:
              resp = resp.json()
              resp_queryid = resp.get("id")

              final_resp.append(f"Successfully pushed query {q_name} to Databricks SQL!, received id: {resp_queryid}")

              ## Now I alter query with the visuals
              try: 
                  if new_query["visualizations"] is None:
                      final_resp.append(f"SUCCESS pushed visualiztions to query {q_name}: No visualization in query")
                  else: 
                    for i, v in enumerate(new_query["visualizations"]):

                        active_visualization = {}
                        active_visualization["name"] = v.get("name")
                        active_visualization["type"] = v.get("type")
                        active_visualization["description"] = v.get("description")
                        active_visualization["options"] = v.get("options")
                        active_visualization["query_id"] = resp_queryid

                        data = json.dumps(active_visualization)
                        resp = requests.post(f"{dbsql_url}visualizations", data=data, headers=headers).json()

                        ### Save old and new visualization Id
                        old_viz_id = v.get("id")
                        new_viz_id = resp.get("id")

                        visualization_id_mappings[old_viz_id] = new_viz_id
                        
                        #final_resp = True
                        final_resp.append(f"SUCCESS pushed visualiztions to query {q_name}!")

              except Exception as e:
                  msg = str(e)
                  er_msg = f"FAIL to push visualizations to query {q_name} with error: {msg}"
                  final_resp.append(er_msg)

          else:
              resp = resp.json()
              msg = resp.get("message")
              er_msg = f"FAIL to push {q_name} with error: {msg}"
              final_resp.append(er_msg)

      except Exception as e:
          msg = str(e)
          er_msg = f"FAIL to push {q_name} with error: {msg}"
          final_resp.append(er_msg)
       
      return_obj = {"visualization_id_mappings":visualization_id_mappings,
                    "run_status_messages": final_resp
                   }
      return return_obj

# COMMAND ----------

# DBTITLE 1,Functions to upload Dashboard objects in a Spark udf in parallel
@udf("string")
def upload_dashboard(raw_dashboard, viz_id_mappings, dbsql_endpoint_id, dbsql_url):
  import requests
  import json

  headers = {"Authorization":f"Bearer {DBX_TOKEN}"}
  dbsql_endpoint_id = str(dbsql_endpoint_id)
  dbsql_url = str(dbsql_url)
  active_dashboard = json.loads(raw_dashboard)
  vizualization_id_mappings = viz_id_mappings

  return_objs = {}  
  final_resp = []
  new_dashboard = {}
  ## No longer need Id, cause DBSQL will generate new Id and stuff
  d_name = active_dashboard.get("name")

  ## Get Mapped data source Id of where you want the query to point to in DBSQL 
  ## (assumes that data source already exists, cant have a query without a Data source)
  new_dashboard = {
                  "name": None,
                  "layout": None,
                  "dashboard_filters_enabled": None,
                  "widgets": None,
                  "is_trashed": None,
                  "is_draft": None,
                  "tags": None
                  }

  new_dashboard["name"] = active_dashboard.get("name")
  new_dashboard["layout"] = active_dashboard.get("layout")
  new_dashboard["dashboard_filters_enabled"] = active_dashboard.get("dashboard_filters_enabled")
  new_dashboard["widgets"] = active_dashboard.get("widgets") or []
  new_dashboard["is_trashed"] = active_dashboard.get("is_trashed")
  new_dashboard["is_draft"] = active_dashboard.get("is_draft")
  new_dashboard["tags"] = ["migrated_from_redash"].append(active_dashboard.get("tags")) or ["migrated_from_redash"]

  ## Submit Query to API in DBSQL
  new_dashboard_json = json.dumps(new_dashboard)
  try:
      resp = requests.post(f"{dbsql_url}dashboards", data=new_dashboard_json, headers=headers).json()
      resp_dashboardid = resp.get("id")

      final_resp.append(f"SUCCESS pushed dashboard {d_name} to Databricks SQL with new Id: {resp_dashboardid} !")
  except Exception as e:
      msg = str(e)
      final_resp.append(f"FAIL to push dashboard {d_name} with error: {msg}")


  ##### For each dashboard, add all the widgets in the dashboard
  try: 
    for widget in new_dashboard["widgets"]:

        active_widget = {"dashboard_id": "",
                        "visualization_id": "",
                        "text":"",
                        "options":"",
                        "width":1
                        }

        #### Need to get new visualization Id from old

        old_id = widget.get("visualization").get("id")
        new_id = visualization_id_mappings.get(old_id)

        active_widget["dashboard_id"] = resp_dashboardid
        active_widget["visualization_id"] = new_id
        active_widget["text"] = widget.get("visualization").get("text")
        active_widget["options"] = widget.get("options") or ""
        active_widget["width"] = widget.get("visualization").get("width") or 1

        data = json.dumps(active_widget)
        resp = requests.post(f"{self.dbsql_url}widgets", data=data, headers=self.headers).json()

        fina_resp.append(f"SUCCESS Added widget with new widget Id: {resp.get('id')}")
  except Exception as e:
    msg = str(e)
    final_resp.append(f"FAIL to push wdiget {resp.get('id')} with error: {msg}")

  return_obj = {"run_status_messages": final_resp}
  return return_obj

# COMMAND ----------

(query_objects
.withColumn("upload_message",upload_queries(col("query_object"), lit(dbsql_endpoint_id_to_use), lit(databricks_sql_url)))
.display()
)

# COMMAND ----------

display(dashboard_objects)
