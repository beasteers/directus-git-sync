import os
import glob
import logging
from . import EXPORT_DIR
from .util import yaml_load
from .api import API
log = logging.getLogger(__name__)


def apply(email, password, url, src_dir=EXPORT_DIR, force: 'bool'=False):
    """Apply Directus schema, flows, websockets, dashboards, and roles to a Directus instance."""
    assert url and email and password, "missing url and credentials"
    log.info(f"Importing Directus schema and flows to {url}")
    log.info(f"Loading from {src_dir}\n")

    api = API(url)
    api.login(email, password)

    log.info("# --------------------------------- Settings --------------------------------- #")
    api.apply_settings(load_dir(f'{src_dir}/settings.yaml'))
    log.info("# ---------------------------------- Schema ---------------------------------- #")
    schema = load_dir(f'{src_dir}/schema.yaml')
    if schema:
        diff = api.diff_schema(schema, force=force)
        if diff:
            api.apply_schema(diff['data'])
    log.info("# ----------------------------------- Flows ---------------------------------- #")
    api.apply_flows(load_dir(f'{src_dir}/flows'))
    api.apply_operations(load_dir(f'{src_dir}/operations'))
    log.info("# -------------------------------- Dashboards -------------------------------- #")
    api.apply_dashboards(load_dir(f'{src_dir}/dashboards'))
    api.apply_panels(load_dir(f'{src_dir}/panels'))
    log.info("# --------------------------------- Webhooks --------------------------------- #")
    api.apply_webhooks(load_dir(f'{src_dir}/webhooks'))
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