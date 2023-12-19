import os
import glob
import logging
from . import EXPORT_DIR, URL, EMAIL, PASSWORD
from .util import yaml_load
from .api import API
log = logging.getLogger(__name__)


def apply(email=EMAIL, password=PASSWORD, url=URL, src_dir=EXPORT_DIR, only=None, force: 'bool'=False):
    """Apply Directus schema, flows, websockets, dashboards, and roles to a Directus instance."""
    assert url and email and password, "missing url and/or credentials"
    log.info(f"Importing Directus schema and flows to {url}")
    log.info(f"Loading from {src_dir}\n")

    api = API(url)
    api.login(email, password)

    # log.info("# --------------------------------- Settings --------------------------------- #")
    # api.apply_settings(load_dir(f'{src_dir}/settings.yaml'))
    if not only or 'schema' in only:
        log.info("# ---------------------------------- Schema ---------------------------------- #")
        schema = load_dir(f'{src_dir}/schema.yaml')
        if schema:
            diff = api.diff_schema(schema, force=force)
            if diff:
                log.info("Applying diff:")
                log.info("Diff Applied: %s", api.apply_schema(diff['data']) or '')
            else:
                log.info("Schema up to date! %s", diff)
    if not only or 'flows' in only:
        log.info("# ----------------------------------- Flows ---------------------------------- #")
        api.apply_flows(load_dir(f'{src_dir}/flows'))
        api.apply_operations(load_dir(f'{src_dir}/operations'))
    if not only or 'dashboards' in only:
        log.info("# -------------------------------- Dashboards -------------------------------- #")
        api.apply_dashboards(load_dir(f'{src_dir}/dashboards'))
        api.apply_panels(load_dir(f'{src_dir}/panels'))
    if not only or 'webhooks' in only:
        log.info("# --------------------------------- Webhooks --------------------------------- #")
        api.apply_webhooks(load_dir(f'{src_dir}/webhooks'))
    if not only or 'roles' in only:
        log.info("# ----------------------------------- Roles ---------------------------------- #")
        api.apply_roles(load_dir(f'{src_dir}/roles'))
        api.apply_permissions(load_dir(f'{src_dir}/permissions'))

def load_dir(src_dir):
    if os.path.isfile(src_dir):
        return yaml_load(src_dir)
    return [yaml_load(f) for f in glob.glob(f'{src_dir}/*')]


if __name__ == '__main__':
    logging.basicConfig()
    import fire
    fire.Fire(apply)