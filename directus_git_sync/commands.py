import json
import os
import glob
import logging
import requests
from . import EXPORT_DIR, URL, EMAIL, PASSWORD
from .util import load_dir, pretty_print_schema_diff
from .util import export_dir, export_one
from .util import load_data, dump_data
from .topo_sort import min_topological_sort, invert_graph
from .api import API
log = logging.getLogger(__name__)


def diff(email=EMAIL, password=PASSWORD, url=URL, src_dir=EXPORT_DIR, only=None, force: 'bool'=False):
    """Diff Directus schema, flows, websockets, dashboards, and roles to a Directus instance."""
    assert url and email and password, "missing url and/or credentials"
    log.info(f"Importing Directus schema and flows to {url}")
    log.info(f"Loading from {src_dir}\n")

    api = API(url)
    api.login(email, password)

    diff = api.diff_unpacked_schema(load_dir(f'{src_dir}/schema', as_dict=True), force=force)
    print('Raw Diff: ', json.dumps(diff, indent=2))
    pretty_print_schema_diff(diff)

    print(":: Done Diffing :) ::")


def apply(email=EMAIL, password=PASSWORD, url=URL, src_dir=EXPORT_DIR, only=None, force: 'bool'=False, yes=True):
    """Apply Directus schema, flows, websockets, dashboards, and roles to a Directus instance."""
    assert url and email and password, "missing url and/or credentials"
    log.info(f"Importing Directus schema and flows to {url}")
    log.info(f"Loading from {src_dir}\n")

    api = API(url)
    api.login(email, password)

    # if not only or 'settings' in only:
    #     api.apply_settings(load_dir(f'{src_dir}/settings.yaml'))
    if not only or 'schema' in only:
        # api.diff_apply_schema(load_dir(f'{src_dir}/schema.yaml'), force=force, yes=yes)
        api.diff_apply_unpacked_schema(load_dir(f'{src_dir}/schema', as_dict=True), force=force, yes=yes)
    if not only or 'flows' in only:
        api.apply_flows(load_dir(f'{src_dir}/flows'))
        api.apply_operations(load_dir(f'{src_dir}/operations'))
    if not only or 'dashboards' in only:
        api.apply_dashboards(load_dir(f'{src_dir}/dashboards'))
        api.apply_panels(load_dir(f'{src_dir}/panels'))
    if not only or 'webhooks' in only:
        api.apply_webhooks(load_dir(f'{src_dir}/webhooks'))
    # if not only or 'roles' in only:
    #     api.apply_roles(load_dir(f'{src_dir}/roles'))
    #     api.apply_permissions(load_dir(f'{src_dir}/permissions'))
    # if not only or 'presets' in only:
    #     api.apply_presets(load_dir(f'{src_dir}/presets'))
    # if not only or 'extensions' in only:
    #     api.apply_extensions(load_dir(f'{src_dir}/extensions'))


def export(email=EMAIL, password=PASSWORD, url=URL, out_dir=EXPORT_DIR):
    '''Dump the configuration of a Directus to disk (to be committed to git).'''
    assert url and email and password, "missing url and credentials"
    log.info(f"Exporting Directus schema and flows from {url}")
    log.info(f"Saving to {out_dir}\n")

    api = API(url)
    api.login(email, password)
    export_one(api.export_settings(), out_dir, 'settings')
    # export_one(api.export_user_mapping(), out_dir, 'users')
    # export_one(api.export_schema(), out_dir, 'schema')
    export_dir(api.export_unpacked_schema(), out_dir, 'schema')
    export_dir(api.export_flows(), out_dir, 'flows')
    export_dir(api.export_operations(), out_dir, 'operations')
    export_dir(api.export_dashboards(), out_dir, 'dashboards')
    export_dir(api.export_panels(), out_dir, 'panels')
    export_dir(api.export_webhooks(), out_dir, 'webhooks')
    # export_dir(api.export_presets(), out_dir, 'presets', ['bookmark', 'collection', 'id'])
    export_dir(api.export_extensions(), out_dir, 'extensions', ['schema.name', 'schema.type'])
    # export_dir(api.export_roles(), out_dir, 'roles')
    export_dir([
        d for d in api.export_permissions()
        if d.get('system') is not True and 'id' in d
    ], out_dir, 'permissions', keys=['role', 'action', 'collection', 'id'])


QUESTIONS = [
    "Are you sure you want to delete all of the flows, operations, webhooks, and roles?",
    "Really? you really sure?",
    "I mean your funeral... last chance!"
]

def wipe(email, password, url=URL):
    '''Wipe all flows, operations, webhooks, and roles from a Directus instance. Used for debugging.'''
    assert url and email and password, "missing url and credentials"
    for q in QUESTIONS:
        if input(f'{q} y/[n]: ').strip().lower() != 'y':
            log.info("Okie! probably for the best.")
            return
    else:
        log.warning("Okay let's destroy everything!")

    log.info(f"Importing Directus schema and flows to {url}")

    api = API(url)
    api.login(email, password)

    api.apply_settings({})
    api.diff_apply_schema({"collections": []})
    api.apply_flows([], allow_delete=True)
    api.apply_operations([], allow_delete=True)
    api.apply_dashboards([], allow_delete=True)
    api.apply_panels([], allow_delete=True)
    api.apply_webhooks([], allow_delete=True)
    api.apply_roles([
        d for d in api.export_roles()['data']
        if d.get('system') is not True and 'id' in d and d.get('admin_access')
    ], allow_delete=True)
    api.apply_permissions([], allow_delete=True)


DROP_FIELDS = ['user_created', 'user_updated']

def data(*collections, email=EMAIL, password=PASSWORD, url=URL, out_dir=os.path.join(EXPORT_DIR, 'data'), drop_fields=DROP_FIELDS, only=None, force: 'bool'=False):
    """Apply Directus schema, flows, websockets, dashboards, and roles to a Directus instance."""
    import tqdm

    assert url and email and password, "missing url and/or credentials"
    log.info(f"Importing Directus schema and flows to {url}")
    log.info(f"Loading from {out_dir}\n")

    api = API(url)
    api.login(email, password)

    if not collections:
        collections = [d['collection'] for d in api.get_collections()['data'] if d.get('meta')]
        collections = [c for c in collections if not c.startswith('directus_')]

    os.makedirs(out_dir, exist_ok=True)
    for c in collections:
        log.info(f"# ----------------------------- {c} ------------------------------ #")
        nrows = int(api.json('GET', f'/items/{c}?aggregate[count]=*')['data'][0]['count'])
        if not nrows:
            log.info(f"{c}: empty")
            continue
        fname = os.path.join(out_dir, f'{c}.json')
        
        log.info(f"{c}: writing {nrows} rows to {fname}")
        items = (
            {k: v for k, v in d.items() if k not in drop_fields} 
            for xs in api.iter_items(c) for d in xs)
        items = list(tqdm.tqdm(items, total=nrows))
        dump_data(items, fname)


def seed(email=EMAIL, password=PASSWORD, url=URL, out_dir=os.path.join(EXPORT_DIR, 'data'), only=None, force: 'bool'=False):
    """Apply Directus schema, flows, websockets, dashboards, and roles to a Directus instance."""

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

    for group in keys:
        for gkey in group:
            collection, key = gkey
            if not key or gkey not in graph_data:
                log.info("Skipping %s", gkey)
                continue
            try:
                log.info('creating %s: %s', gkey, api.create_items(collection, graph_data[gkey]))
            except requests.exceptions.HTTPError as e:
                log.info('updated %s: %s', gkey, api.update_item(collection, key, graph_data[gkey]))

# import ipdb
# @ipdb.iex
def main(key=None):
    logging.basicConfig()
    import fire
    fire.Fire({
        "diff": diff,
        "apply": apply,
        "export": export,
        "wipe": wipe,
        "data": data,
        "seed": seed,
        # "api": API,
    })


if __name__ == '__main__':
    main()