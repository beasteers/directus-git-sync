import io
import json
import requests
import logging
from . import URL, EMAIL, PASSWORD
from .util import dict_diff, status_text, pretty_print_schema_diff, get_key
from .topo_sort import create_graph_from_items, min_topological_sort
log = logging.getLogger(__name__.split('.')[0])
# log.setLevel(logging.DEBUG)

def localhost_subdomain_hotfix(url):  # FIXME: port
    from urllib.parse import urlparse
    p = urlparse(url)
    return (p._replace(netloc=f'localhost:{p.port}').geturl(), {'Host': p.hostname}) if p.hostname.endswith('localhost') else (url, {})

class API:
    """An interface to the directus API."""
    def __init__(self, url=URL, email=None, password=None):
        self.url, self.headers = url, {} #localhost_subdomain_hotfix(url)
        if email:
            password = password or input("Directus password pls: ")
            if password:
                self.login(email, password)

    def login(self, email=EMAIL, password=PASSWORD):
        """Login to directus to get an access token."""
        # Authenticate and get access token
        response = requests.post(f'{self.url}/auth/login', json={"email": email, "password": password}, headers=self.headers)
        response.raise_for_status()
        self.access_token = response.json()["data"]["access_token"]
        self.headers['Authorization'] = f"Bearer {self.access_token}"
        return self

    def json(self, method, path, raw=False, **kw):
        log.debug(f'🐦 ↑{method} {path} %s', kw)
        r = requests.request(method, f"{self.url}{path}", headers={**self.headers, **(kw.get('headers') or {})}, **kw)
        log.debug(f'{"🟢" if r.ok else "🔴"} ↓{method} {path} {r.status_code} {r.content}')
        try:
            r.raise_for_status()
            if raw:
                return r.content
            elif r.content:
                return r.json()
        except requests.exceptions.HTTPError as e:
            log.error('%s: %s', r.status_code, r.content.decode())
            raise
        except requests.exceptions.JSONDecodeError as e:
            raise ValueError(f"Could not read: {r.content}")

    # --------------------------------- Settings --------------------------------- #

    def export_settings(self):
        """Get settings."""
        return self.json('GET', '/settings')['data']
    
    def apply_settings(self, settings, **kw):
        """Update server settings."""
        current = self.export_settings()
        if settings == current:
            log.info("%-11s :: %s.", 'Settings', status_text('unchanged'))
            return
        log.info("%-11s :: %s.", 'Settings', status_text('modified'))
        return self.json('PATCH', '/settings', json=settings, **kw)

    # ---------------------------------- Schema ---------------------------------- #

    def export_schema(self):
        """Get schema object."""
        schema = self.json('GET', '/schema/snapshot')['data']
        schema = sanitize_schema_null_collections(schema)
        return schema

    def diff_schema(self, schema, force=False):
        # https://docs.directus.io/reference/system/schema.html#retrieve-schema-difference
        schema = sanitize_schema_null_collections(schema)
        diff = self.json('POST', '/schema/diff', params={"force": force}, json=schema)
        return diff['data'] if diff else None
    
    def apply_schema(self, schema_diff):
        # https://docs.directus.io/reference/system/schema.html#apply-schema-difference
        result = self.json('POST', '/schema/apply', json=schema_diff)
        log.info("Schema :: \033[93mdiff applied.\033[0m")
        return result
    
    def diff_apply_schema(self, schema, force=False, yes=False):
        if schema:
            diff = self.diff_schema(schema, force=force)
            # print(diff)
            has_changes = pretty_print_schema_diff(diff or {}, confirm_delete=not yes)
            if not has_changes:
                log.info("Schema      :: \033[92mup to date!\033[0m")
                return
            
            try:
                return self.apply_schema(diff)
            except requests.exceptions.HTTPError as e:
                log.error("Schema      :: \033[91merror applying diff.\033[0m")
                log.error(e.response.content)
                raise

    # ---------------------------------- Folders --------------------------------- #

    def export_folders(self):
        """Get all folders"""
        return self.json('GET', '/folders')['data']

    def apply_folders(self, items, **kw):
        """Update server with folders configurations."""
        return self._apply('/folders', items, **kw)

    # ----------------------------------- Flows ---------------------------------- #
    
    def export_operations(self):
        """Get all operations"""
        return self.json('GET', '/operations')['data']
    
    def export_flows(self):
        """Get all flows"""
        return self.json('GET', '/flows')['data']
    
    def apply_operations(self, items, **kw):
        """Update server with operations configurations."""
        return self._apply('/operations', items, **kw)

    def apply_flows(self, items, **kw):
        """Update server with flows configurations."""
        # https://docs.directus.io/reference/system/flows.html#update-multiple-flows
        return self._apply('/flows', items, forbidden_keys=['operations'], **kw)
    
    # --------------------------------- Webhooks --------------------------------- #
    
    def export_webhooks(self):
        """Get all webhooks"""
        return self.json('GET', '/webhooks')['data']
    
    def apply_webhooks(self, items, **kw):
        """Update server with webhooks configurations."""
        return self._apply('/webhooks', items, **kw)
    
    # -------------------------------- Dashboards -------------------------------- #
    
    def export_panels(self):
        """Get all panels"""
        return self.json('GET', '/panels')['data']
    
    def export_dashboards(self):
        """Get all dashboards"""
        return self.json('GET', '/dashboards')['data']
    
    def apply_panels(self, items, **kw):
        """Update server with panels configurations."""
        return self._apply('/panels', items, **kw)
    
    def apply_dashboards(self, items, **kw):
        """Update server with dashboards configurations."""
        return self._apply('/dashboards', items, forbidden_keys=['panels'], desc_keys=['name'], **kw)

    # ----------------------------------- Roles ---------------------------------- #
    
    def export_roles(self):
        """Get all roles"""
        return self.json('GET', '/roles')['data']

    def export_permissions(self):
        """Get all permissions"""
        return self.json('GET', '/permissions')['data']
    
    def export_users(self):
        """Get all users"""
        return self.json('GET', '/users')['data']
    
    def apply_roles(self, items, **kw):
        """Update server with roles configurations."""
        return self._apply('/roles', items, forbidden_keys=['users'], desc_keys=['name'], **kw)
    
    def apply_permissions(self, items, **kw):
        """Update server with permissions configurations."""
        return self._apply('/permissions', items, desc_keys=['action', 'collection', 'fields'], **kw)
    
    def apply_users(self, items, **kw):
        """Update server with users configurations."""
        return self._apply('/users', items, **kw)
    
    # ----------------------------------- Misc ----------------------------------- #

    def export_graphql_sdl(self):
        """Get graphql sdl"""
        return self.json('GET', '/server/specs/graphql/', raw=True)

    # ---------------------------------- Import ---------------------------------- #


    def _apply(self, route, items, existing=None, forbidden_keys=None, allow_delete=True, desc_keys=None):
        if existing is None:
            existing = self.json('GET', route)['data']
        existing = {d['id']: d for d in existing if 'id' in d}
        items = {d['id']: d for d in items}
        log.debug(f'items {route} {set(items)}')
        log.debug(f'existing {route} {set(existing)}')

        for d in [items, existing]:
            for k in d:
                for ki in ['user_created']:  # XXX: is this desired? it's needed when copying between instances but we're losing this information
                    d[k].pop(ki, None)
                for ki in (forbidden_keys or []):
                        d[k].pop(ki, None)

        # check for new
        new = set(items) - set(existing)
        log.debug(f'new {route} {new}')
        if new:
            new_b4 = new
            new_graph = create_graph_from_items({k: items[k] for k in new}, "id")
            new = min_topological_sort(new_graph, flat=True)
            assert set(new)==set(new_b4)
            log.info(f"🌱 Creating {route}: {new}")
            # self.json('POST', route, json=[items[k] for k in new])
            failed = []
            for k in new:
                try:
                    self.json('POST', route, json=items[k])
                except requests.exceptions.HTTPError:
                    failed.append(k)
            for k in failed:
                self.json('POST', route, json=items[k])

        # check for changes
        in_common = set(items) & set(existing)
        diffs = {k: dict_diff(existing[k], items[k]) for k in in_common}
        update = {k for k in in_common if any(diffs[k])}
        unchanged = in_common - update
        log.debug(f'diffs {route} {diffs}')
        log.debug(f'update {route} {update}')

        if update:
            log.info(f"🔧 Updating {route}: {update}")
            for k in update:
                self.json('PATCH', f'{route}/{k}', json=items[k])
        
        # check for deletions
        missing = set(existing) - set(items)
        delete = missing if allow_delete else set()
        log.debug(f'missing {route} {missing}')
        log.debug(f'delete {route} {delete}')
        if '/roles' in route:  # FIXME: this is janky
            delete = [k for k in delete if existing[k].get('admin_access') != True]
        if delete:
            log.warning(f"🗑 Deleting {route}: {delete}")
            self.json('DELETE', route, json=list(delete))
        elif missing and not allow_delete:
            log.warning(f"Missing (skipping delete) {route}: {missing}")

        # summary
        title = route.strip('/').replace('/', '|').title()
        log.info(
            "%-11s :: %s. %s. %s. %s.", 
            title,
            status_text('new', i=len(new)), 
            status_text('modified', i=len(update)), 
            status_text('deleted', i=len(delete)),
            status_text('unchanged', i=len(unchanged)),
        )
        if desc_keys:
            for k in new:
                log.info("%-11s :: %s", title, status_text('new', ' . '.join(f"{get_key(items[k], *dk.split('.'))}" for dk in desc_keys)))
            for k in update:
                log.info("%-11s :: %s", title, status_text('modified', ' . '.join(f"{get_key(items[k], *dk.split('.'))}" for dk in desc_keys)))
            for k in delete:
                log.warning("%-11s :: %s", title, status_text('deleted', ' . '.join(f"{get_key(existing[k], *dk.split('.'))}" for dk in desc_keys)))
            # for k in unchanged:
            #     log.warning("%-11s :: %s", title, status_text('unchanged', ' . '.join(f"{get_key(existing[k], *dk.split('.'))}" for dk in desc_keys)))
        return new, update, delete, unchanged
    
    # -------------------------------- Collections ------------------------------- #

    def get_collections(self):
        return self.json('GET', '/collections')['data']

    # ---------------------------------------------------------------------------- #
    #                             Data synchronization                             #
    # ---------------------------------------------------------------------------- #
    
    def get_items(self, collection, **kw):
        return [x for xs in self.iter_items(collection, **kw) for x in xs]

    def iter_items(self, collection, batch=100, limit=None, search=None):
        offset = 0
        while True:
            items = self.json(
                'SEARCH' if search else 'GET', 
                f'/items/{collection}', 
                params={'limit': batch, 'offset': offset},
                json=search)
            items = items['data']
            offset += len(items)
            if limit and offset >= limit:
                yield items[:limit - offset or None]
                return
            if not items:
                break
            yield items

    def create_items(self, collection, data):
        return self.json('POST', f'/items/{collection}', json=data)
    
    def update_item(self, collection, key, data):
        return self.json('PATCH', f'/items/{collection}/{key}', json=data)

    # def update_items(self, collection, data):
    #     return [
    #         self.json('PATCH', f'/items/{collection}/{d["id"]}', json=d)
    #         for d in data
    #     ]

    def delete_items(self, collection, ids):
        return self.json('DELETE', f'/items/{collection}', json=ids)
    
    def import_data(self, collection, fname):

        fields = self.json('GET', f'/fields/{collection}')['data']
        field = [x for x in fields if (x.get('schema') or {}).get('is_primary_key')]
        primary_key = field[0]['field']

        with open(fname, 'r') as f:
            data = json.load(f)
        graph = {d[primary_key]: d for d in data}
        new_graph = create_graph_from_items(graph, "id")
        keys = min_topological_sort(new_graph, flat=True)
        data = [graph[k] for k in keys]

        return self.json('POST', f'/items/{collection}')

        # ext = fname.split('.')[-1]
        # mime = ({'json': 'application/json', 'csv': 'text/csv'})[ext.lower()]
        # return self.json('POST', f'/utils/import/{collection}', files={
        #     'file': (f'{collection}.{ext}', io.StringIO(json.dumps(data)), mime)
        # })
    
    # def import_data(self, collection, data):
    #     return self.json('POST', f'/utils/import/{collection}', files={
    #         'file': (f'{collection}.json', io.StringIO(json.dumps(data)), "application/json")
    #     })
    
    # def import_csv(self, collection, data_str):
    #     return self.json('POST', f'/utils/import/{collection}', files={
    #         'file': (f'{collection}.csv', io.StringIO(data_str), "text/csv")
    #     })
    
    async def data_sync(self, collections):
        import uuid
        import websockets
        import json
        while True:
            try:
                async with websockets.connect(self.url) as websocket:
                    # Authenticate
                    log.info("Authenticating...")
                    await websocket.send(json.dumps({
                        "type": "auth",
                        "access_token": self.access_token
                    }))
                    log.info("Authenticated!")

                    # Subscribe to collections
                    uuids = {
                        str(uuid.uuid4()): c
                        for c in collections
                    }
                    for uid, collection in uuids.items():
                        log.info("Subscribing to %s (%s)", collection, uid)
                        await websocket.send(json.dumps({
                            "type": "subscribe",
                            "collection": collection,
                            "uid": uid,
                        }))
                    

                    # Listen for incoming messages
                    # {
                    #     "type": "subscription",
                    #     "event": "create",
                    #     "data": [ ... ]
                    # }
                    async for message in websocket:
                        data = json.loads(message)
                        event = message.get('event')
                        if event == 'init':
                            log.info("Subscription init: %s", message)
                            continue
                        
                        collection = uuids[message['uid']]
                        if event == 'create':
                            self.create_items(collection, data['data'])
                        elif event == 'update':
                            self.update_items(collection, data['data'])
                        elif event == 'delete':
                            self.delete_items(collection, [d['id'] for d in data['data']])

                        print({"event": "onmessage", "data": data})
            except Exception as e:
                log.exception(e)


def sanitize_schema_null_collections(schema):
    dropped_collections = [c for c in schema['collections'] if c.get('meta', {}) is None]
    dropped_relations = [c for c in schema['relations'] if c.get('meta', {}) is None]
    
    schema['collections'] = [c for c in schema['collections'] if c.get('meta', {}) is not None]
    schema['relations'] = [c for c in schema['relations'] if c.get('meta', {}) is not None]
    
    if dropped_collections:
        print('Ignoring collections:', [c['collection'] for c in dropped_collections])
    if dropped_relations:
        print('Ignoring relations:', [f'{c["collection"]}.{c.get("field")}' for c in dropped_relations])
    
    return schema

# if __name__ == "__main__":
#     asyncio.get_event_loop().run_until_complete(main())