import requests
import logging
log = logging.getLogger(__name__)


class API:
    """An interface to the directus API."""
    def __init__(self, url):
        self.url = url
        self.headers = {}

    def login(self, email, password):
        """Login to directus to get an access token."""
        # Authenticate and get access token
        response = requests.post(f'{self.url}/auth/login', json={"email": email, "password": password})
        response.raise_for_status()
        access_token = response.json()["data"]["access_token"]
        self.headers = {"Authorization": f"Bearer {access_token}"}

    def json(self, method, path, raw=False, **kw):
        log.debug(f'🐦 ↑{method} {path} %s', kw)
        r = requests.request(method, f"{self.url}{path}", headers={**self.headers, **(kw.get('headers') or {})}, **kw)
        log.debug(f'{"🟢" if r.ok else "🔴"} ↓{method} {path} {r.status_code} {r.content}')
        r.raise_for_status()
        try:
            return r.content if raw else r.json() if r.content else None
        except requests.exceptions.JSONDecodeError as e:
            raise ValueError(f"Could not read: {r.content}")

    # ---------------------------------- Export ---------------------------------- #

    def export_settings(self):
        """Get settings."""
        return self.json('GET', '/settings')

    def export_schema(self):
        """Get schema object."""
        return self.json('GET', '/schema/snapshot')
    
    def export_operations(self):
        """Get all operations"""
        return self.json('GET', '/operations')
    
    def export_flows(self):
        """Get all flows"""
        return self.json('GET', '/flows')
    
    def export_webhooks(self):
        """Get all webhooks"""
        return self.json('GET', '/webhooks')
    
    def export_panels(self):
        """Get all panels"""
        return self.json('GET', '/panels')
    
    def export_dashboards(self):
        """Get all dashboards"""
        return self.json('GET', '/dashboards')
    
    def export_roles(self):
        """Get all roles"""
        return self.json('GET', '/roles')

    def export_permissions(self):
        """Get all permissions"""
        return self.json('GET', '/permissions')
    
    def export_folders(self):
        """Get all folders"""
        return self.json('GET', '/folders')
    
    def export_graphql_sdl(self):
        """Get graphql sdl"""
        return self.json('GET', '/server/specs/graphql/', raw=True)

    # ---------------------------------- Import ---------------------------------- #

    def apply_settings(self, settings, **kw):
        """Update server settings."""
        return self.json('PATCH', '/settings', json=settings, **kw)

    def diff_schema(self, schema, force=False):
        # https://docs.directus.io/reference/system/schema.html#retrieve-schema-difference
        return self.json('POST', '/schema/diff', params={"force": force}, json=schema)
    
    def apply_schema(self, schema_diff):
        # https://docs.directus.io/reference/system/schema.html#apply-schema-difference
        return self.json('POST', '/schema/apply', json=schema_diff)

    def _apply(self, route, items, existing=None, forbidden_keys=None, allow_delete=True):
        if existing is None:
            existing = self.json('GET', route)['data']
        existing = {d['id']: d for d in existing if 'id' in d}
        items = {d['id']: d for d in items}
        log.debug(f'items {route} {set(items)}')
        log.debug(f'existing {route} {set(existing)}')

        for k in items:
            for ki in ['user_created']:  # XXX: is this desired? it's needed when copying between instances but we're losing this information
                items[k].pop(ki, None)
            for ki in (forbidden_keys or []):
                    items[k].pop(ki, None)

        new = set(items) - set(existing)
        log.debug(f'new {route} {new}')
        if new:
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
                    
        
        update = set(items) & set(existing)
        log.debug(f'update {route} {update}')
        if update:
            log.info(f"🔧 Updating {route}: {update}")
            for k in update:
                self.json('PATCH', f'{route}/{k}', json=items[k])
        
        missing = set(existing) - set(items)
        delete = missing if allow_delete else set()
        log.debug(f'missing {route} {missing}')
        log.debug(f'delete {route} {delete}')
        if delete:
            log.warning(f"🗑 Deleting {route}: {delete}")
            if '/roles' in route:  # FIXME: this is janky
                delete = [k for k in delete if existing[k].get('admin_access') != True]
            self.json('DELETE', route, json=list(delete))
        elif missing:
            log.warning(f"Missing (skipping delete) {route}: {missing}")
        return new, update, delete
    
    def apply_operations(self, items, **kw):
        """Update server with operations configurations."""
        return self._apply('/operations', items, **kw)

    def apply_flows(self, items, **kw):
        """Update server with flows configurations."""
        # https://docs.directus.io/reference/system/flows.html#update-multiple-flows
        return self._apply('/flows', items, forbidden_keys=['operations'], **kw)
    
    def apply_webhooks(self, items, **kw):
        """Update server with webhooks configurations."""
        return self._apply('/webhooks', items, **kw)
    
    def apply_panels(self, items, **kw):
        """Update server with panels configurations."""
        return self._apply('/panels', items, **kw)
    
    def apply_dashboards(self, items, **kw):
        """Update server with dashboards configurations."""
        return self._apply('/dashboards', items, forbidden_keys=['panels'], **kw)
    
    def apply_roles(self, items, **kw):
        """Update server with roles configurations."""
        return self._apply('/roles', items, forbidden_keys=['users'], **kw)
    
    def apply_permissions(self, items, **kw):
        """Update server with permissions configurations."""
        return self._apply('/permissions', items, **kw)
    
    def apply_folders(self, items, **kw):
        """Update server with folders configurations."""
        return self._apply('/folders', items, **kw)