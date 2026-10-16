"""File helpers shared by everything that writes state."""
from __future__ import annotations

import os
import time


def replace_with_retry(src: str, dst: str, attempts: int = 6) -> None:
    """os.replace that waits and retries when the target is briefly held by another process (Windows: a scanner or an
    indexer). The rename itself is atomic, so a reader sees the old file or the new one, never half of one."""
    for attempt in range(attempts):
        try:
            os.replace(src, dst)
            return
        except PermissionError:
            if attempt == attempts - 1:
                raise
            time.sleep(0.02 * (attempt + 1))
