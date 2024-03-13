import os
import glob
import logging
from . import EXPORT_DIR, URL, EMAIL, PASSWORD
from .util import load_dir
from .api import API
log = logging.getLogger(__name__)


def apply(email=EMAIL, password=PASSWORD, url=URL, src_dir=EXPORT_DIR, only=None, force: 'bool'=False):
    """Apply Directus schema, flows, websockets, dashboards, and roles to a Directus instance."""
    assert url and email and password, "missing url and/or credentials"
    log.info(f"Importing Directus schema and flows to {url}")
    log.info(f"Loading from {src_dir}\n")

    api = API(url)
    api.login(email, password)

    # if not only or 'settings' in only:
    #     api.apply_settings(load_dir(f'{src_dir}/settings.yaml'))
    if not only or 'schema' in only:
        api.diff_apply_schema(load_dir(f'{src_dir}/schema.yaml'), force=force)
    if not only or 'flows' in only:
        api.apply_flows(load_dir(f'{src_dir}/flows'))
        api.apply_operations(load_dir(f'{src_dir}/operations'))
    if not only or 'dashboards' in only:
        api.apply_dashboards(load_dir(f'{src_dir}/dashboards'))
        api.apply_panels(load_dir(f'{src_dir}/panels'))
    if not only or 'webhooks' in only:
        api.apply_webhooks(load_dir(f'{src_dir}/webhooks'))
    if not only or 'roles' in only:
        api.apply_roles(load_dir(f'{src_dir}/roles'))
        api.apply_permissions(load_dir(f'{src_dir}/permissions'))


if __name__ == '__main__':
    logging.basicConfig()
    import fire
    fire.Fire(apply)