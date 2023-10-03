import os
import logging
log = logging.getLogger(__name__)
log.setLevel(getattr(logging, os.getenv("LOG_LEVEL") or "INFO"))

from .api import API
from . import util
from .export import export
from .apply import apply
from .wipe import wipe

def cli():
    logging.basicConfig()
    # import fire
    # fire.Fire
    from starstar.argparse import Star
    Star({
        "apply": apply,
        "export": export,
        "wipe": wipe,
        # "api": API,
    }, description="""
Managing Directus as a git repository!
    """, env_format="DIRECTUS")