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


RESOURCE_CONFIG = {
    'policies': {'forbidden_keys': ['users', 'roles']},
    'roles': {'forbidden_keys': ['users', 'children']},
    'permissions': {},
    'flows': {'forbidden_keys': ['operations']},
    'operations': {},
    'dashboards': {'forbidden_keys': ['panels']},
    'panels': {},
    'presets': {},
    'webhooks': {},
}
SETTINGS_IGNORED = {'id', 'project_id'}
OPTIONAL_RESOURCES = {'panels', 'webhooks'}


def _load_configuration(src_dir):
    required = ['settings.yaml', 'schema'] + [
        name for name in RESOURCE_CONFIG if name not in OPTIONAL_RESOURCES
    ] + ['extensions']
    missing = [name for name in required if not os.path.exists(os.path.join(src_dir, name))]
    if missing:
        raise ValueError('incomplete Directus snapshot; missing: ' + ', '.join(missing))

    resources = {
        name: load_dir(os.path.join(src_dir, name))
        for name in RESOURCE_CONFIG
    }
    policies = {str(item['id']) for item in resources['policies']}
    referenced = {
        str(policy)
        for role in resources['roles']
        for policy in role.get('policies', [])
    } | {
        str(item['policy'])
        for item in resources['permissions']
        if item.get('policy')
    }
    if referenced - policies:
        raise ValueError(
            'snapshot references policies that were not exported: '
            + ', '.join(sorted(referenced - policies)))
    if any(role.get('users') for role in resources['roles']):
        raise ValueError('snapshot contains environment-specific role user bindings')
    if any(item.get('user') for item in resources['presets']):
        raise ValueError('snapshot contains user-scoped presets')

    return {
        'settings': load_data(os.path.join(src_dir, 'settings.yaml')),
        'schema': load_dir(os.path.join(src_dir, 'schema'), as_dict=True),
        'resources': resources,
        'extensions': load_dir(os.path.join(src_dir, 'extensions')),
    }


def _contains_delete(value):
    if isinstance(value, dict):
        return value.get('kind') == 'D' or any(_contains_delete(item) for item in value.values())
    if isinstance(value, list):
        return any(_contains_delete(item) for item in value)
    return False


def build_plan(api, src_dir=EXPORT_DIR, force=False):
    """Return a complete, non-mutating application-state plan."""
    desired = _load_configuration(src_dir)
    schema = api.diff_unpacked_schema(desired['schema'], force=force)
    current_settings = api.export_settings()
    desired_settings = {
        key: value for key, value in desired['settings'].items()
        if key not in SETTINGS_IGNORED
    }
    settings = {
        key: {'current': current_settings.get(key), 'desired': value}
        for key, value in desired_settings.items()
        if current_settings.get(key) != value
    }
    policy_ids = {str(item['id']) for item in desired['resources']['policies']}

    def managed_actual(name):
        items = api.json('GET', f'/{name}')['data']
        if name == 'policies':
            return [item for item in items if not item.get('admin_access')]
        if name == 'roles':
            return [
                item for item in items
                if set(map(str, item.get('policies', []))).issubset(policy_ids)
            ]
        if name == 'permissions':
            return [
                item for item in items
                if not item.get('system') and str(item.get('policy')) in policy_ids
            ]
        if name == 'presets':
            return [item for item in items if not item.get('user')]
        return items

    resources = {
        name: api.diff_items(
            f'/{name}',
            items,
            existing=managed_actual(name),
            forbidden_keys=config.get('forbidden_keys'))
        for name, config in RESOURCE_CONFIG.items()
        for items in [desired['resources'][name]]
    }
    installed = {
        item.get('schema', {}).get('name'): item.get('schema', {}).get('version')
        for item in api.export_extensions()
    }
    extensions_missing = sorted(
        f"{item.get('schema', {}).get('name')}@{item.get('schema', {}).get('version')}"
        for item in desired['extensions']
        if installed.get(item.get('schema', {}).get('name'))
        != item.get('schema', {}).get('version'))
    destructive = _contains_delete(schema) or any(
        changes['delete'] for changes in resources.values())
    has_changes = bool(schema or settings or extensions_missing) or any(
        any(changes.values()) for changes in resources.values())
    return {
        'schema': schema,
        'settings': settings,
        'resources': resources,
        'extensions_missing': extensions_missing,
        'destructive': bool(destructive),
        'has_changes': bool(has_changes),
    }


def diff(email=EMAIL, password=PASSWORD, url=URL, src_dir=EXPORT_DIR, force: 'bool'=False, output=None):
    """Plan all managed Directus configuration without changing the server."""
    assert url and email and password, "missing url and/or credentials"
    log.info(f"Planning Directus configuration for {url}")
    log.info(f"Loading from {src_dir}\n")

    api = API(url)
    api.login(email, password)

    result = build_plan(api, src_dir, force=force)
    rendered = json.dumps(result, indent=2, sort_keys=True)
    if output:
        with open(output, 'w') as stream:
            stream.write(rendered + '\n')
    print(rendered)
    return result


def apply(email=EMAIL, password=PASSWORD, url=URL, src_dir=EXPORT_DIR, force: 'bool'=False, yes: 'bool'=False):
    """Apply all managed Directus configuration after an explicit approval."""
    assert url and email and password, "missing url and/or credentials"
    if not yes:
        raise ValueError('refusing apply without --yes after reviewing directus-git-sync diff')
    log.info(f"Applying Directus configuration to {url}")
    log.info(f"Loading from {src_dir}\n")

    api = API(url)
    api.login(email, password)

    desired = _load_configuration(src_dir)
    before = build_plan(api, src_dir, force=force)
    if before['extensions_missing']:
        raise ValueError(
            'required extension builds are not installed: '
            + ', '.join(before['extensions_missing']))
    api.diff_apply_unpacked_schema(desired['schema'], force=force, yes=True)
    api.apply_settings({
        key: value for key, value in desired['settings'].items()
        if key not in SETTINGS_IGNORED
    })

    # Create and update dependencies first, then delete in reverse dependency
    # order. This avoids deleting a policy while a permission still refers to it.
    for name, config in RESOURCE_CONFIG.items():
        getattr(api, f'apply_{name}')(
            desired['resources'][name],
            allow_delete=False)
    for name, config in reversed(RESOURCE_CONFIG.items()):
        getattr(api, f'apply_{name}')(
            desired['resources'][name],
            allow_delete=True)

    after = build_plan(api, src_dir, force=force)
    if after['has_changes']:
        raise RuntimeError('Directus configuration did not converge: ' + json.dumps(after, sort_keys=True))
    return after


def export(email=EMAIL, password=PASSWORD, url=URL, out_dir=EXPORT_DIR):
    '''Dump the configuration of a Directus to disk (to be committed to git).'''
    assert url and email and password, "missing url and credentials"
    log.info(f"Exporting Directus schema and flows from {url}")
    log.info(f"Saving to {out_dir}\n")

    api = API(url)
    api.login(email, password)
    os.makedirs(out_dir, exist_ok=True)
    for name in list(RESOURCE_CONFIG) + ['schema', 'extensions']:
        os.makedirs(os.path.join(out_dir, name), exist_ok=True)
    export_one({
        key: value for key, value in api.export_settings().items()
        if key not in SETTINGS_IGNORED
    }, out_dir, 'settings')
    # export_one(api.export_user_mapping(), out_dir, 'users')
    # export_one(api.export_schema(), out_dir, 'schema')
    export_dir(api.export_unpacked_schema(), out_dir, 'schema')
    export_dir(api.export_flows(), out_dir, 'flows')
    export_dir(api.export_operations(), out_dir, 'operations')
    export_dir(api.export_dashboards(), out_dir, 'dashboards')
    export_dir(api.export_panels(), out_dir, 'panels')
    export_dir(api.export_webhooks(), out_dir, 'webhooks')
    export_dir([
        item for item in api.export_presets()
        if not item.get('user')
    ], out_dir, 'presets', ['bookmark', 'collection', 'id'])
    export_dir(api.export_extensions(), out_dir, 'extensions', ['schema.name', 'schema.type'])
    policies = [
        {key: value for key, value in item.items() if key not in ['users', 'roles']}
        for item in api.export_policies()
        if not item.get('admin_access')
    ]
    policy_ids = {str(item['id']) for item in policies}
    export_dir(policies, out_dir, 'policies', ['name', 'id'])
    export_dir([
        {key: value for key, value in item.items() if key not in ['users', 'children']}
        for item in api.export_roles()
        if set(map(str, item.get('policies', []))).issubset(policy_ids)
    ], out_dir, 'roles', ['name', 'id'])
    export_dir([
        d for d in api.export_permissions()
        if d.get('system') is not True and 'id' in d
        and str(d.get('policy')) in policy_ids
    ], out_dir, 'permissions', keys=['policy', 'action', 'collection', 'id'])


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
