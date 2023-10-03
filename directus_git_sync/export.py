import os
import logging
from . import EXPORT_DIR, URL
from .api import API
from .util import yaml_dump, str_dump
log = logging.getLogger(__name__)


def export(email, password, url=URL, out_dir=EXPORT_DIR):
    '''Dump the configuration of a Directus to disk (to be committed to git).'''
    assert url and email and password, "missing url and credentials"
    log.info(f"Exporting Directus schema and flows from {url}")
    log.info(f"Saving to {out_dir}\n")

    api = API(url)
    api.login(email, password)
    log.info("# ----------------------------------- Flows ---------------------------------- #")
    yaml_dump(api.export_settings()['data'], out_dir, 'settings')
    log.info("# ---------------------------------- Schema ---------------------------------- #")
    yaml_dump(api.export_schema()['data'], out_dir, 'schema')
    log.info("# ----------------------------------- Flows ---------------------------------- #")
    dump_each(api.export_flows()['data'], out_dir, 'flows')
    dump_each(api.export_operations()['data'], out_dir, 'operations')
    log.info("# -------------------------------- Dashboards -------------------------------- #")
    dump_each(api.export_dashboards()['data'], out_dir, 'dashboards')
    dump_each(api.export_panels()['data'], out_dir, 'panels')
    log.info("# --------------------------------- Webhooks --------------------------------- #")
    dump_each(api.export_webhooks()['data'], out_dir, 'webhooks')
    log.info("# ----------------------------------- Roles ---------------------------------- #")
    dump_each(api.export_roles()['data'], out_dir, 'roles')
    dump_each([
        d for d in api.export_permissions()['data']
        if d.get('system') is not True and 'id' in d
    ], out_dir, 'permissions', keys=['role', 'action', 'collection', 'id'])

    # # not used for migration - purely for git diff niceness
    # log.info("# -------------------------------- GraphQL SDL ------------------------------- #")
    # str_dump(api.export_graphql_sdl().decode(), out_dir, 'schema.graphql')

def dump_each(data, out_dir, name, keys=['name', 'id']):
    for i, d in enumerate(data):
        yaml_dump(d, f'{out_dir}/{name}', '-'.join(f'{d.get(k)}' for k in keys) if keys else f'{i}')


if __name__ == '__main__':
    logging.basicConfig()
    import fire
    fire.Fire(export)