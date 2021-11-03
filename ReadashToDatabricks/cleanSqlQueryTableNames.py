import json
import re

with open("redash_export/all_redash_queries test.json", "r") as r:
    queries = json.load(r)

with open("mappings/tablemappings.json") as mm:
    tables_mappings = json.load(mm)

with open("mappings/functionmappings.json") as ff:
    function_mappings = json.load(ff)

##### Perform REGEX replace statements on all query definitions to replace old table names with new table names

def multiple_replace(string, rep_dict):
    pattern = re.compile("|".join([re.escape(k.lower()) for k in sorted(rep_dict,key=len,reverse=True)]), flags=re.DOTALL)
    return pattern.sub(lambda x: rep_dict[x.group(0)], string.lower())


for query in queries:

    source_string = queries.get(query).get("query")
    
    new_query = multiple_replace(source_string, tables_mappings)
    print(f"Completed table mappings for query: {query}")
    new_query = multiple_replace(new_query, function_mappings)
    print(f"Completed function mappings for query: {query}")

    queries[query]["query"] = new_query

r.close()
mm.close()
ff.close()

##### Save cleaned queries file, do not overwrite source redash files

with open('redash_export/all_redash_queries_cleaned.json', 'w', encoding='utf-8') as f:
    json.dump(queries, f, ensure_ascii=False, indent=4)

print(f"Queries have been updated!")