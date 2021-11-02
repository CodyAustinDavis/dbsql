import requests
import json


with open ('config.json', "r") as f:

    config = json.load(f)


dbsql_url = config["databricks_sql_url"]
dbsql_token = config["databricks_sql_token"]
headers = {"Authorization": f"Bearer {dbsql_token}"}




#resp = requests.post(f"{dbsql_url}queries", headers=headers).json()

with open('redash_export/all_redash_queries.json') as qq:
    queries = json.load(qq)

with open('data_source_mappings.json', 'r') as ds:
    data_source_mappings = json.load(ds)

with open('redash_export/all_redash_dashboards.json') as dd:
    dashboards = json.load(dd)

##### Run through all redash queries and post to DBSQL with the mapped data sources, includes visuals, BUT does treat it as a new query

for q, i in enumerate(queries):

    new_query = {}
    ## No longer need Id, cause DBSQL will generate new Id and stuff
    active_query = queries.get(i)
    q_name = active_query.get("name")

    ## Get Mapped data source Id of where you want the query to point to in DBSQL (assumes that data source already exists, cant have a query without a Data source)

    redash_data_source_id = active_query.get("data_source_id")
    #print(f"Redash Data Source Id {redash_data_source_id}")

    dbsql_data_source_id = data_source_mappings.get("redash_data_sources").get(str(redash_data_source_id)).get("dbsql_data_source_id")
    print(f"Pushing Redash Query: {q_name} from Redash Data Source Id: {redash_data_source_id} to DataSQL Data Source Id: {dbsql_data_source_id}...")


    new_query["data_source_id"] = dbsql_data_source_id
    new_query["query"] = active_query.get("query")
    new_query["name"] = active_query.get("name")
    new_query["description"] = active_query.get("description") or ""
    new_query["schedule"] = active_query.get("schedule")
    new_query["options"] = active_query.get("options")
    new_query["visualizations"] = [{"id": active_query.get("visualizations")[0].get("id"),
                                  "type": active_query.get("visualizations")[0].get("type"),
                                  "name": active_query.get("visualizations")[0].get("name"),
                                  "description": active_query.get("visualizations")[0].get("description"),
                                  "options": active_query.get("visualizations")[0].get("options"),
                                  "updated_at": active_query.get("visualizations")[0].get("updated_at"),
                                  "created_at": active_query.get("visualizations")[0].get("created_at")
                                }]

    ## Submit Query to API in DBSQL, must create the query, get the Id, and then alter the query with its Id to add everything else (visuals)
    new_query_json = json.dumps(new_query)
    try:
        resp = requests.post(f"{dbsql_url}queries", data=new_query_json, headers=headers).json()
        resp_queryid = resp.get("query_id")

        print(resp)
        print(f"Successfully pushed query {q_name} to Databricks SQL!, received id: {resp_queryid}")
        
        ## Now I alter query with the visuals

        resp = requests.post(f"{dbsql_url}queries/{resp_queryid}", data=new_query_json, headers=headers).json()

        print(f"Successfully added visualizations for query {q_name} in DBSQL!")

    except Exception as e:
        msg = str(e)
        print(f"Failed to push {q_name} with error: {msg}")



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








