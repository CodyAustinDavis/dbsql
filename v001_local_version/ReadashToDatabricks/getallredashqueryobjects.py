import click
import requests
from redash_toolbelt.client import Redash
import json

template = u"""/*
Name: {name}
Data source: {data_source}
Created By: {created_by}
Last Updated At: {last_updated_at}
*/
{query}"""

with open ('config.json', "r") as f:

    config = json.load(f)

def save_full_queries(queries_json):

    ### This gets each query, along with its visualizations, and sends each query to json by query Id
    filename = "redash_export/all_redash_queries.json"
    with open(filename, "w") as f:

        content = json.dumps(queries_json)

        f.write(content)


def save_full_dashboards(dashboards_json):

    ### This gets each query, along with its visualizations, and sends each query to json by query Id
    filename = "redash_export/all_redash_dashboards.json"
    with open(filename, "w") as f:

        content = json.dumps(dashboards_json)

        f.write(content)


@click.command()
@click.argument("redash_url", default=config.get("redash_url"))
@click.argument(
    "api_key",
    required=True,
    default=config.get("redash_api_key"),
    envvar="REDASH_API_KEY"
)

def main(redash_url, api_key):
    redash = Redash(redash_url, api_key)
    queries = redash.paginate(redash.queries)
    dashboards = redash.paginate(redash.dashboards)
    datasources = redash.get_data_sources

    print(f"Data Sources: {datasources}")
    print(f"Dashboards: {dashboards}")

    #### Build full json here
    full_query_json = {}

    for query in queries:
        query_id = query["id"]
        query_results = redash.get_query(query_id)

        full_query_json[query_id] = query_results

    save_full_queries(full_query_json)

    ##### Now get dashboard objects
    
    full_dashboards_json = {}

    for dashboard in dashboards:
        dashboard_id = dashboard["slug"]
        dashboard_results = redash.get_dashboard(dashboard_id)

        full_dashboards_json[dashboard_id] = dashboard_results

    save_full_dashboards(full_dashboards_json)
    

if __name__ == "__main__":
    
    main()

    f.close()