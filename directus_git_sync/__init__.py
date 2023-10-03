import os
import logging
log = logging.getLogger(__name__)
log.setLevel(getattr(logging, os.getenv("LOG_LEVEL") or "INFO"))

URL = os.getenv("DIRECTUS_URL") or "http://localhost:8055"

EXPORT_DIR = 'export'
REPO = os.getenv("GITSYNC_REPO")
LINK = os.getenv("GITSYNC_LINK")
ROOT = os.getenv("GITSYNC_ROOT")
if not LINK and REPO:
    LINK = REPO.split('/')[-1].removesuffix('.git')
if ROOT and LINK:
    EXPORT_DIR = os.path.join(ROOT or '/git', LINK)

from .api import API
from . import util
from .export import export
from .apply import apply
from .wipe import wipe
