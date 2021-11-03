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
4. Visualizations Ids are global across queries, I need to test this assumption!!

### TO DO:

> Check for duplicate query names, and handle by either overwriting or creating new query name - v2
> Handle the fully qualified table naming changes - i.e. map all table names to a database and rename queries (stretch goal)
> Handle layout data
"""

import requests
import json

with open ('config.json', "r") as f:
    config = json.load(f)


def main(config):

    config = config

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



    visualization_id_mappings = {}

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
                        
                        ### Save old and new visualization Id
                        old_viz_id = v.get("id")
                        new_viz_id = resp.get("id")

                        visualization_id_mappings[old_viz_id] = new_viz_id

                        print(f"Added Visualization with old id: {old_viz_id} and generated new Id: {new_viz_id}")

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

    ##### Upload all dashboards associated with the widgets which are tied to the visualizations

    for d, i in enumerate(dashboards):

        new_dashboard = {}
        ## No longer need Id, cause DBSQL will generate new Id and stuff
        active_dashboard = dashboards.get(i)
        d_name = active_dashboard.get("name")

        ## Get Mapped data source Id of where you want the query to point to in DBSQL (assumes that data source already exists, cant have a query without a Data source)
        new_dashboard = {
                        "name": None,
                        "layout": None,
                        "dashboard_filters_enabled": None,
                        "widgets": None,
                        "is_trashed": None,
                        "is_draft": None,
                        "tags": None
                        }
    
        #print(f"Redash Data Source Id {redash_data_source_id}")

        print(f"Pushing Redash Dashboard: {d_name} from Redash to DataSQL...")

        new_dashboard["name"] = active_dashboard.get("name")
        new_dashboard["layout"] = active_dashboard.get("layout")
        new_dashboard["dashboard_filters_enabled"] = active_dashboard.get("dashboard_filters_enabled")
        new_dashboard["widgets"] = active_dashboard.get("widgets") or []
        new_dashboard["is_trashed"] = active_dashboard.get("is_trashed")
        new_dashboard["is_draft"] = active_dashboard.get("is_draft")
        new_dashboard["tags"] = active_dashboard.get("tags") or ["Migrated from Redash"]

        ## Submit Query to API in DBSQL
        new_dashboard_json = json.dumps(new_dashboard)
        try:
            resp = requests.post(f"{dbsql_url}dashboards", data=new_dashboard_json, headers=headers).json()
            resp_dashboardid = resp.get("id")

            print(f"Successfully pushed dashboard {d_name} to Databricks SQL with new Id: {resp_dashboardid} !")
        except Exception as e:
            msg = str(e)
            print(f"Failed to push dashboard {d_name} with error: {msg}")


        ##### For each dashboard, add all the widgets in the dashboard

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
            resp = requests.post(f"{dbsql_url}widgets", data=data, headers=headers).json()

            print(f"Added widget with new widget Id: {resp.get('id')}")




if __name__ == "__main__":

    clean_or_raw = input("Run in the following query modes cleaned|raw : ")
    main(config)

    f.close()





