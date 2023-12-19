import os
import csv
import glob
import logging
import requests
from . import EXPORT_DIR, URL, EMAIL, PASSWORD
from .util import load_data
from .topo_sort import min_topological_sort, invert_graph
from .api import API
log = logging.getLogger(__name__)



# [
#     ('deployments', {
#         'deployment_id': 'asdfsadf-asdfass-adsfasf',
#         'dev_id': 'fs-0001',
#     }),
#     ('deployments', {
#         'deployment_id': 'asdfsadf-asdfass-adsfasf',
#         'dev_id': None,
#     }),
#     ('sensors', {
#         'dev_id': 'fs-0001',
#         'wowow': 'asdfsadf-asdfass-adsfasf',
#     }),
#     ('sensors', {
#         'dev_id': 'fs-0003',
#     }),
# ]



# {
#     'deployments': ('deployments', {'dev_id': ('sensors', 'dev_id')}),
#     'sensors': {},
# }


def get_schema_topo(fields):
    fields = [f for f in fields if f.get('schema')]
    pkey = next((f['field'] for f in fields if f['schema'].get('is_primary_key', False)), None)
    relations = {
        f['field']: (f['schema']['foreign_key_table'], f['schema']['foreign_key_column']) 
        for f in fields 
        if f['schema'].get('foreign_key_table') and f['schema'].get('foreign_key_column')}
    return pkey, relations

def get_collection_graph(data, topo):
    graph = {}
    graph_data = {}
    for collection, rows in data.items():
        pkey, relations = topo[collection]
        if pkey is None:
            continue
        for row in rows:
            # FIXME: right now we are ignoring the foreign key column and assuming it's the primary key
            #        otherwise we would need to have multiple indexes??
            k = (collection, row[pkey])
            graph[k] = {
                (f_table, row[col])
                for col, (f_table, f_col) in relations.items()
                if row.get(col) is not None
            }
            graph_data[k] = row
    return graph, graph_data


def seed(email=EMAIL, password=PASSWORD, url=URL, out_dir=os.path.join(EXPORT_DIR, 'data'), only=None, force: 'bool'=False):
    """Apply Directus schema, flows, websockets, dashboards, and roles to a Directus instance."""
    import tqdm

    assert url and email and password, "missing url and/or credentials"
    log.info(f"Importing Directus data to {url}")
    log.info(f"Loading from {out_dir}\n")

    api = API(url)
    api.login(email, password)

    # get collection topology
    data = {
        os.path.splitext(os.path.basename(f))[0]: load_data(f)
        for f in glob.glob(os.path.join(out_dir, '*'))
    }
    collection_topo = {
        c: get_schema_topo(api.json('get', f'/fields/{c}')['data'])
        for c in data
    }
    # graph contains key -> set of dependent keys
    # graph_data contains key -> data row
    graph, graph_data = get_collection_graph(data, collection_topo)
    keys = min_topological_sort(graph, flat=False)
    # print(len(keys))
    # print([len(x) for x in keys])
    # from IPython import embed
    # if input('>?'):embed()

    for group in keys:
        for gkey in group:
            collection, key = gkey
            if not key or gkey not in graph_data:
                print("Skipping", gkey)
                continue
            try:
                print('creating', gkey, api.create_items(collection, graph_data[gkey]))
            except requests.exceptions.HTTPError as e:
                print("updated", gkey, api.update_item(collection, key, graph_data[gkey]))
                
        # input()


if __name__ == '__main__':
    logging.basicConfig()
    import fire
    fire.Fire(seed)