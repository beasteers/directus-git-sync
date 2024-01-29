import os
import logging
from . import EXPORT_DIR, URL, EMAIL, PASSWORD
from .api import API
from .util import export_dir, export_one
log = logging.getLogger(__name__)


def export(email=EMAIL, password=PASSWORD, url=URL, out_dir=EXPORT_DIR):
    '''Dump the configuration of a Directus to disk (to be committed to git).'''
    assert url and email and password, "missing url and credentials"
    log.info(f"Exporting Directus schema and flows from {url}")
    log.info(f"Saving to {out_dir}\n")

    api = API(url)
    api.login(email, password)
    export_one(api.export_settings(), out_dir, 'settings')
    export_one(api.export_schema(), out_dir, 'schema')
    export_dir(api.export_flows(), out_dir, 'flows')
    export_dir(api.export_operations(), out_dir, 'operations')
    export_dir(api.export_dashboards(), out_dir, 'dashboards')
    export_dir(api.export_panels(), out_dir, 'panels')
    export_dir(api.export_webhooks(), out_dir, 'webhooks')
    export_dir(api.export_roles(), out_dir, 'roles')
    export_dir([
        d for d in api.export_permissions()
        if d.get('system') is not True and 'id' in d
    ], out_dir, 'permissions', keys=['role', 'action', 'collection', 'id'])


if __name__ == '__main__':
    logging.basicConfig()
    import fire
    fire.Fire(export)