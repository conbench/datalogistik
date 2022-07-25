import logging

from .config import debug

logging.basicConfig(
    format="%(levelname)s [%(asctime)s] %(message)s",
    level=logging.DEBUG if debug else logging.INFO,
)
log = logging.getLogger(__name__)
