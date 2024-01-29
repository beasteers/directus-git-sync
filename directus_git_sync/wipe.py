import os
import glob
import logging
from . import URL
from .api import API
log = logging.getLogger(__name__)

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


if __name__ == '__main__':
    logging.basicConfig()
    import fire
    fire.Fire(wipe)