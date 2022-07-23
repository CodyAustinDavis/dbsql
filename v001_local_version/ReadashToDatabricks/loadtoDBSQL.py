"""
Author: Cody Austin Davis
Creation Date: 11.1.2021
Use Case: Migration for all query and dashboard objects from a self managed redash intances into a DatabricksSQL managed enviornment. 

#### ! Only handles up to 250 queries and dashboard objects for right now ####
Steps:

1. Gets data source Id to insert into 
    > Uploads query objects 
        > Add visualizations 
            > Uploads dashboard object shells 
                > Add widgets

#### Functionality Notes: 

1. module can be imported and accessed ad hoc for more interactively add and remove/change objects
2. Can change the run mode if running main method

Assumptions: 
1. All endpoint names are unique
2. All data_source_ids from the self hosted redash will be converged into a single endpoint to ensure queries work
3. Query objects must be created for visualziations to exist, and dashboard shells must exists before widgets can be inserted into them
4. Visualizations Ids are global across queries, I need to test this assumption!!

### TO DO:

> Check for duplicate query names, and handle by either overwriting or creating new query name - v2
> Handle the fully qualified table naming changes - i.e. map all table names to a database and rename queries (stretch goal)
> Handle layout data
> Handle uploading of whole dashboards with associated visualizations and queries automatically

"""

import requests
import json


class DatabricksSQLMigration():

    def __init__(self, config, clean_or_raw = "raw"):


        self.config = config
        self.dbsql_url = config["databricks_sql_url"]
        self.dbsql_token = config["databricks_sql_token"]
        self.dbsql_endpoint_name_to_use = config["databricks_sql_endpoint_name"]
        self.headers = {"Authorization": f"Bearer {self.dbsql_token}"}
        self.visualization_id_mappings = {}

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

        endp_resp = requests.get("https://e2-demo-field-eng.cloud.databricks.com/api/2.0/preview/sql/data_sources", headers=self.headers).json()

        if self.dbsql_endpoint_name_to_use is not None: 

            ### If multiple matches, just pick one
            self.dbsql_endpoint_id_to_use = str([endpoint.get("endpoint_id") for endpoint in endp_resp if endpoint.get("name") == self.dbsql_endpoint_name_to_use][0])
            self.dbsql_data_source_id = str([endpoint.get("id") for endpoint in endp_resp if endpoint.get("name") == self.dbsql_endpoint_name_to_use][0])

            print(f"Running Migration from Redash to DBSQL on endpoint: {self.dbsql_endpoint_name_to_use} with data_source_id = {self.dbsql_data_source_id} and endpoint_id = {self.dbsql_endpoint_id_to_use}")
        else: 
            raise ConfigException('Please specify a valid endpoint name to migrate to!') 


        ### Change to all_redash_queries_cleaned.json parameter

        if clean_or_raw == "cleaned":
            try:
                with open('redash_export/all_redash_queries_cleaned.json') as qq:
                    self.queries = json.load(qq)
            except Exception as e:
                msg = f"Error: Unable to find or load CLEANED queries from Redash, make sure queries object exists before getting started... \n {str(e)}"
                ConfigException(msg)

        elif clean_or_raw == "raw":
            try:
                with open('redash_export/all_redash_queries.json') as qq:
                    self.queries = json.load(qq)
            except Exception as e:
                msg = f"Error: Unable to find or load RAW queries from Redash, make sure queries object exists before getting started... \n {str(e)}"
                ConfigException(msg)
        else:
            print("No proprer query config given, provide cleaned|raw")


        with open('redash_export/all_redash_dashboards.json') as dd:
            try:
                self.dashboards = json.load(dd)
            except Exception as e:
                msg = f"Error: Unable to find or load dashboards from Redash, make sure queries object exists before getting started... \n {str(e)}"
                ConfigException(msg)


    ##### Create Operations

    ##### Run All Queries
    ## If mode == "all", ignores subset param, otherwise uses subset param to get subset of queries
    def upload_queries(self, mode="all", query_name_subset=['']):

        for q, i in enumerate(self.queries):

            new_query = {}
            ## No longer need Id, cause DBSQL will generate new Id and stuff
            active_query = self.queries.get(i)
            q_name = active_query.get("name")

            #### trigger run mode and query subset
            if mode == "all":
                pass
            else: 
                if q_name not in query_name_subset:
                    continue

            ## Get Mapped data source Id of where you want the query to point to in DBSQL (assumes that data source already exists, cant have a query without a Data source)

            redash_data_source_id = active_query.get("data_source_id")
            #print(f"Redash Data Source Id {redash_data_source_id}")

            print(f"Pushing Redash Query: {q_name} from Redash Data Source Id: {redash_data_source_id} to DataSQL Data Source Id: {self.dbsql_data_source_id}... (endpoint Id of : {self.dbsql_endpoint_id_to_use})")


            new_query["data_source_id"] = self.dbsql_data_source_id
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
                resp = requests.post(f"{self.dbsql_url}queries", data=new_query_json, headers=self.headers)

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
                            resp = requests.post(f"{self.dbsql_url}visualizations", data=data, headers=self.headers).json()
                            
                            ### Save old and new visualization Id
                            old_viz_id = v.get("id")
                            new_viz_id = resp.get("id")

                            self.visualization_id_mappings[old_viz_id] = new_viz_id

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

        return


    ############# Migrate Dashboard and Widget obects #############

    ##### Upload all dashboards associated with the widgets which are tied to the visualizations
    def upload_dashboards(self, mode="all", dashboard_name_subset=['']):

        for d, i in enumerate(self.dashboards):

            new_dashboard = {}
            ## No longer need Id, cause DBSQL will generate new Id and stuff
            active_dashboard = self.dashboards.get(i)
            d_name = active_dashboard.get("name")

            #### trigger run mode and query subset
            if mode == "all":
                pass
            else: 
                if d_name not in dashboard_name_subset:
                    continue


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
            new_dashboard["tags"] = ["migrated_from_redash"].append(active_dashboard.get("tags")) or ["migrated_from_redash"]

            ## Submit Query to API in DBSQL
            new_dashboard_json = json.dumps(new_dashboard)
            try:
                resp = requests.post(f"{self.dbsql_url}dashboards", data=new_dashboard_json, headers=self.headers).json()
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
                new_id = self.visualization_id_mappings.get(old_id)

                active_widget["dashboard_id"] = resp_dashboardid
                active_widget["visualization_id"] = new_id
                active_widget["text"] = widget.get("visualization").get("text")
                active_widget["options"] = widget.get("options") or ""
                active_widget["width"] = widget.get("visualization").get("width") or 1

                data = json.dumps(active_widget)
                resp = requests.post(f"{self.dbsql_url}widgets", data=data, headers=self.headers).json()

                print(f"Added widget with new widget Id: {resp.get('id')}")



    ###### Remove Operations
    
    def remove_all_queries(self):

        resp = requests.get(f"{self.dbsql_url}queries", headers=self.headers, params={"page_size":250}).json()
        query_names = [i.get('name') for i in resp.get('results')]
        print(f"Query Objects to Delete: {resp.get('count')} compared to array size: {len(query_names)} \n")
        print(f"Deleting query names: {query_names}")

        for index, query in enumerate(resp["results"]):
            delete_response = requests.delete(f"{self.dbsql_url}queries/{query.get('id')}", headers=self.headers)
        
            if delete_response.status_code == 200:
                print(f"Successfully deleted query: {query.get('name')}")
            else: 
                resp = delete_response.json()
                msg = resp.get("message")
                print(f"Failed to delete query {query.get('name')} with error code: {delete_response.status_code} \n Error: {msg}")



    def remove_selected_queries(self, tags=[], query_names=[]):
        resp = requests.get(f"{self.dbsql_url}queries", headers=self.headers, params={"page_size":250}).json()
        queries_to_delete = list(set([i.get('name') for i in resp.get('results') if (len(list(set(i.get('tags')) & set(tags))) > 0 or len(list(set([i.get('name')]) & set(query_names))) > 0)]))
        print(f"Query Objects to Delete: {resp.get('count')} total compared to delete size: {len(queries_to_delete)} \n")
        print(f"Deleting query names: {queries_to_delete}")

        for index, query in enumerate(resp["results"]):
            ### This should delete all query ids with same name copy (this could be a problem but yes)
            if query.get('name') in queries_to_delete:
                
                print(f"Deleting query: {query.get('name')} with id {query.get('id')}")
                
                delete_response = requests.delete(f"{self.dbsql_url}queries/{query.get('id')}", headers=self.headers)

                if delete_response.status_code == 200:
                    print(f"Successfully deleted query: {query.get('name')}")
                else: 
                    resp = delete_response.json()
                    msg = resp.get("message")
                    print(f"Failed to delete query {query.get('name')} with error code: {delete_response.status_code} \n Error: {msg}")                
                


    def remove_all_dashboards(self):
        ### !!! This is VERY dangerous without filters !!!
        resp = requests.get(f"{self.dbsql_url}dashboards", headers=self.headers, params={"page_size":250}).json()
        dashboards_to_delete = [i.get('name') for i in resp.get('results')]

        print(f"Dashboard objects to Delete: {resp.get('count')} total compared to delete  size {len(dashboards_to_delete)} \n")
        print(f"Deleting dashboard names: {dashboards_to_delete}")

        for index, dashboard in enumerate(resp["results"]):
            if dashboard.get('name') in dashboards_to_delete:
                print(f"Deleting dashboard... {dashboard.get('name')} with id: {dashboard.get('id')}")

                delete_response = requests.delete(f"{self.dbsql_url}/dashboards/{dashboard.get('id')}", headers=self.headers)

                if delete_response.status_code == 200:
                    print(f"Successfully deleted dashboard: {dashboard.get('name')}")
                else: 
                    resp = delete_response.json()
                    msg = resp.get("message")
                    print(f"Failed to delete dashboard {dashboard.get('name')} with error code: {delete_response.status_code} \n Error: {msg}")



    def remove_selected_dashboards(self, tags=[], dashboard_names=[]):
        tags = []
        ### !!! This is VERY dangerous without filters !!!
        resp = requests.get(f"{self.dbsql_url}dashboards", headers=self.headers, params={"page_size":250}).json()
        dashboards_to_delete = [i.get('name') for i in resp.get('results') if (len(list(set(i.get('tags')) & set(tags))) > 0 or len(list(set([i.get('name')]) & set(dashboard_names))) > 0)]

        print(f"Dashboard objects to Delete: {resp.get('count')} compared to array size {len(dashboards_to_delete)} \n")
        print(f"Deleting Dashboards names: {dashboards_to_delete}")

        for index, dashboard in enumerate(resp["results"]):
            if dashboard.get('name') in dashboards_to_delete:
                print(f"Deleting dashboard... {dashboard.get('name')} with id: {dashboard.get('id')}")

                delete_response = requests.delete(f"{self.dbsql_url}/dashboards/{dashboard.get('id')}", headers=self.headers)

                if delete_response.status_code == 200:
                    print(f"Successfully deleted dashboard: {dashboard.get('name')}")
                else: 
                    resp = delete_response.json()
                    msg = resp.get("message")
                    print(f"Failed to delete dashboard {dashboard.get('name')} with error code: {delete_response.status_code} \n Error: {msg}")




def main(clean_or_raw, run_mode="all", operation_mode = "add", remove_mode="tag", query_name_list = [''], dashboard_name_list = [''], delete_tag_name_list=['migrated_from_redash']):

    with open ('config.json', "r") as f:
        config = json.load(f)


    migrationInstance = DatabricksSQLMigration(config, clean_or_raw = clean_or_raw)

    if operation_mode == "add":

        migrationInstance.upload_queries(mode=run_mode, query_name_subset=query_name_list)
        migrationInstance.upload_dashboards(mode=run_mode, dashboard_name_subset=dashboard_name_list)

    elif operation_mode == "remove":

        if remove_mode == "tag":

            migrationInstance.remove_selected_queries(tags=delete_tag_name_list)
            migrationInstance.remove_selected_dashboards(tags=delete_tag_name_list)

        elif remove_mode == "name":

            migrationInstance.remove_selected_queries(query_names=query_name_list)
            migrationInstance.remove_selected_dashboards(query_names=dashboard_name_list)

    ## This subset methods take in an array or tag or query/dashboard names, the tags array default to ["migrated_from_redash"]
    
    #migrationInstance.uploadQueries()
    #migrationInstance.uploadDashboards()

    #### Can pass in a list of tags AND a list of query/dashboard names
    #migrationInstance.remove_selected_queries(tags=delete_tag_name_list)
    #migrationInstance.remove_selected_queries(query_names=["TestSalesQuery"])
    #
    #migrationInstance.remove_selected_queries()
    f.close()

if __name__ == "__main__":

    clean_or_raw = input("Run in the following query modes cleaned|raw : ").strip()
    all_or_subset = input("Run mode: all|subset :").strip()
    add_or_remove = input("Operation mode: add|remove :").strip()

    if add_or_remove == "remove":
        remove_mode = input("Run mode: tag|names :").strip()

        if remove_mode == "tag":
            tag_names = [i.strip() for i in input("Provide list of Tag names (csv) - leave blank if none: ").split(",")]

    query_name_list = []
    dashboard_name_list = []

    if all_or_subset == "subset":
        query_name_list = [i.strip() for i in input("Provide list of query names (csv) - leave blank if none: ").split(",")]
        dashboard_name_list = [i.strip() for i in input("Provider list of dashboard names (csv) - leave blank if none: ").split(",")]

        print(f"Uploading Queries: {query_name_list}")
        print(f"Uploading Dashboards: {dashboard_name_list}")

    if all_or_subset == "all":
        print("Uploading all queries and dashboards")

    main(clean_or_raw, all_or_subset, add_or_remove, remove_mode, query_name_list, dashboard_name_list, tag_names)





