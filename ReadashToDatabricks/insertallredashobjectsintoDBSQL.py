"""
Author: Cody Austin Davis
Creation Date: 11.1.2021
Use Case: Migration for all query and dashboard objects from a self managed redash intances into a DatabricksSQL managed enviornment. 

Steps:

1. Gets data source Id to insert into 
    > Uploads query objects 
        > Add visualizations 
            > Uploads dashboard object shells 
                > Add widgets

Assumptions: 
1. All endpoint names are unique
2. All data_source_ids from the self hosted redash will be converged into a single endpoint to ensure queries work
3. Query objects must be created for visualziations to exist, and dashboard shells must exists before widgets can be inserted into them


### TO DO:

> Check for duplicate query names, and handle by either overwriting or creating new query name - v2
> Handle the fully qualified table naming changes - i.e. map all table names to a database and rename queries (stretch goal)

"""

import requests
import json

with open ('config.json', "r") as f:
    config = json.load(f)


dbsql_url = config["databricks_sql_url"]
dbsql_token = config["databricks_sql_token"]
dbsql_endpoint_name_to_use = config["databricks_sql_endpoint_name"]

headers = {"Authorization": f"Bearer {dbsql_token}"}

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


##### Get the Data source Id and endpoint id of the cluster you are going to assign all these queries to at first (data_source_id will soon be decoupled from queries)

endp_resp = requests.get("https://e2-demo-field-eng.cloud.databricks.com/api/2.0/preview/sql/data_sources", headers=headers).json()

if dbsql_endpoint_name_to_use is not None: 

    ### If multiple matches, just pick one
    dbsql_endpoint_id_to_use = str([endpoint.get("endpoint_id") for endpoint in endp_resp if endpoint.get("name") == dbsql_endpoint_name_to_use][0])
    dbsql_data_source_id = str([endpoint.get("id") for endpoint in endp_resp if endpoint.get("name") == dbsql_endpoint_name_to_use][0])

    print(f"Running Migration from Redash to DBSQL on endpoint: {dbsql_endpoint_name_to_use} with data_source_id = {dbsql_data_source_id} and endpoint_id = {dbsql_endpoint_id_to_use}")
else: 
    raise ConfigException('Please specify a valid endpoint name to migrate to!') 


with open('redash_export/all_redash_queries.json') as qq:
    queries = json.load(qq)


with open('redash_export/all_redash_dashboards.json') as dd:
    dashboards = json.load(dd)



############# Migrate Query and visualization obects #############

##### Run through all redash queries and post to DBSQL with the mapped data sources, includes visuals, BUT does treat it as a new query


for q, i in enumerate(queries):

    new_query = {}
    ## No longer need Id, cause DBSQL will generate new Id and stuff
    active_query = queries.get(i)
    q_name = active_query.get("name")

    ## Get Mapped data source Id of where you want the query to point to in DBSQL (assumes that data source already exists, cant have a query without a Data source)

    redash_data_source_id = active_query.get("data_source_id")
    #print(f"Redash Data Source Id {redash_data_source_id}")

    print(f"Pushing Redash Query: {q_name} from Redash Data Source Id: {redash_data_source_id} to DataSQL Data Source Id: {dbsql_data_source_id}... (endpoint Id of : {dbsql_endpoint_id_to_use})")


    new_query["data_source_id"] = dbsql_data_source_id
    new_query["query"] = active_query.get("query")
    new_query["name"] = active_query.get("name")
    new_query["description"] = active_query.get("description") or ""
    new_query["schedule"] = active_query.get("schedule")
    new_query["options"] = active_query.get("options")
    new_query["visualizations"] = active_query.get("visualizations")
                                

    ## Submit Query to API in DBSQL, must create the query, get the Id, and then alter the query with its Id to add everything else (visuals)
    new_query_json = json.dumps(new_query)

    #print(new_query_json)

    try:
        resp = requests.post(f"{dbsql_url}queries", data=new_query_json, headers=headers)

        if resp.status_code == 200:
            resp = resp.json()
            resp_queryid = resp.get("id")

            print(f"Successfully pushed query {q_name} to Databricks SQL!, received id: {resp_queryid}")
            
            ## Now I alter query with the visuals
            try: 

                for i, v in enumerate(new_query["visualizations"]):
                    
                    active_visualization = {}
                    active_visualization["name"] = v.get("name")
                    active_visualization["type"] = v.get("type")
                    active_visualization["description"] = v.get("description")
                    active_visualization["options"] = v.get("options")
                    active_visualization["query_id"] = resp_queryid

                    data = json.dumps(active_visualization)
                    resp = requests.post(f"{dbsql_url}visualizations", data=data, headers=headers).json()

                print(f"Successfully pushed visualiztions to query {q_name}!")

            except Exception as e:
                msg = str(e)
                print(f"Failed to push visualizations to query {q_name} with error: {msg}")

        else:
            resp = resp.json()
            msg = resp.get("message")
            print(f"Failed to push {q_name} with error: {msg}")

    except Exception as e:
        msg = str(e)
        print(f"Failed to push {q_name} with error: {msg}")


############# Migrate Dashboard and Widget obects #############

#### Upload all dashboards associated with the queries after they finish
"""
for d, i in enumerate(dashboards):

    new_dashboard = {}
    ## No longer need Id, cause DBSQL will generate new Id and stuff
    active_dashboard = dashboards.get(i)
    d_name = active_dashboard.get("name")

    ## Get Mapped data source Id of where you want the query to point to in DBSQL (assumes that data source already exists, cant have a query without a Data source)

    redash_data_source_id = active_dashboard.get("data_source_id")
    #print(f"Redash Data Source Id {redash_data_source_id}")

    dbsql_data_source_id = data_source_mappings.get("redash_data_sources").get(str(redash_data_source_id)).get("dbsql_data_source_id")
    print(f"Pushing Redash Query: {q_name} from Redash Data Source Id: {redash_data_source_id} to DataSQL Data Source Id: {dbsql_data_source_id}...")

    new_query["data_source_id"] = dbsql_data_source_id
    new_query["query"] = active_query.get("query")
    new_query["name"] = active_query.get("name")
    new_query["description"] = active_query.get("description") or ""
    new_query["schedule"] = active_query.get("schedule")
    new_query["options"] = active_query.get("options")
    new_query["visualizations"] = active_query.get("visualizations")

    ## Submit Query to API in DBSQL
    new_query_json = json.dumps(new_query)
    try:
        requests.post(f"{dbsql_url}queries", data=new_query_json, headers=headers)
        print(f"Successfully pushed query {q_name} to Databricks SQL!")
    except Exception as e:
        msg = str(e)
        print(f"Failed to push {q_name} with error: {msg}")

"""








