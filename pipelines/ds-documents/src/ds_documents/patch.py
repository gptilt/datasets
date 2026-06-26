import re
from pathlib import Path

# Matches: 25.22, 26.06, 14.3, patch-26-06.
PATCH_RE = re.compile(r"\b(patch[- _]?)?(\d{2,2}\.\d{1,2})\b", re.IGNORECASE)

def infer_patch_hint(uri: str, text_sample: str = "") -> str | None:
    # 1. Try filename first (most reliable signal)
    filename = Path(uri).stem
    m = PATCH_RE.search(filename)
    if m:
        return m.group(2)

    # 2. Try first 2000 chars of content
    m = PATCH_RE.search(text_sample[:2000])
    if m:
        return m.group(2)

    return None