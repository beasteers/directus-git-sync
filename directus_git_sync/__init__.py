import os
import logging
log = logging.getLogger(__name__)
log.setLevel(getattr(logging, os.getenv("LOG_LEVEL") or "INFO"))

URL = os.getenv("DIRECTUS_URL") or "http://localhost:8055"
EMAIL = os.getenv("DIRECTUS_EMAIL") or "admin@example.com"
PASSWORD = os.getenv("DIRECTUS_PASSWORD") or "password"

EXPORT_DIR = os.getenv("DIRECTUS_OUT_DIR")
REPO = os.getenv("GITSYNC_REPO")
LINK = os.getenv("GITSYNC_LINK")
ROOT = os.getenv("GITSYNC_ROOT")
if not LINK and REPO:
    LINK = REPO.split('/')[-1].removesuffix('.git')
if not EXPORT_DIR and ROOT and LINK:
    EXPORT_DIR = os.path.join(ROOT or '/git', LINK)
EXPORT_DIR = EXPORT_DIR or 'directus'

from . import util
from .api import API
from .commands import diff, apply, export, wipe, data, seed
# from .export import export
# from .apply import apply, diff
# from .wipe import wipe
# from .data import data
# from .seed import seed
