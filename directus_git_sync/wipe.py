import os
import glob
import logging
from .util import yaml_load
from .apply import load_dir
from .api import API
log = logging.getLogger(__name__)

QUESTIONS = [
    "Are you sure you want to delete all of the flows, operations, webhooks, and roles?",
    "Really? you really sure?",
    "I mean your funeral... last chance!"
]

def wipe(email, password, url, src_dir='export'):
    '''Wipe all flows, operations, webhooks, and roles from a Directus instance. Used for debugging.'''
    assert url and email and password, "missing url and credentials"
    for q in QUESTIONS:
        if input(f'{q} y/[n]: ').strip().lower() != 'y':
            print("Okie! probably for the best.")
            return
    else:
        print("Okay let's destroy everything!")

    log.info(f"Importing Directus schema and flows to {url}")
    log.info(f"Loading from {src_dir}\n")

    api = API(url)
    api.login(email, password)

    api.apply_settings({})
    # log.info("# ---------------------------------- Schema ---------------------------------- #")
    # diff = api.diff_schema({"collections": []})
    # if diff:
    #     api.apply_schema(diff['data'])
    log.info("# ----------------------------------- Flows ---------------------------------- #")
    api.apply_flows([], allow_delete=True)
    api.apply_operations([], allow_delete=True)
    log.info("# -------------------------------- Dashboards -------------------------------- #")
    api.apply_dashboards([], allow_delete=True)
    api.apply_panels([], allow_delete=True)
    log.info("# --------------------------------- Webhooks --------------------------------- #")
    api.apply_webhooks([], allow_delete=True)
    # log.info("# ----------------------------------- Roles ---------------------------------- #")
    api.apply_roles([
        d for d in load_dir(f'{src_dir}/roles')
        if d.get('admin_access')
    ], allow_delete=True)
    api.apply_permissions([], allow_delete=True)


if __name__ == '__main__':
    logging.basicConfig()
    import fire
    fire.Fire(wipe)