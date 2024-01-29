import os
import glob
import logging
from . import EXPORT_DIR, URL, EMAIL, PASSWORD
from .util import write_data
from .api import API
log = logging.getLogger(__name__)


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
        write_data(items, fname)

if __name__ == '__main__':
    logging.basicConfig()
    import fire
    fire.Fire(apply)