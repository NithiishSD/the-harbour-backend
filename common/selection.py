"""What the viewer chose to monitor inside the deployment's country: data/monitor_selection.json.

The country is MONITOR_COUNTRY and cannot be changed. The ports to monitor and the source countries (every
port of a source country counts) are the viewer's choice; the dashboard writes them here and the live feed
reads them, so enabling a country also enables its coasts in the AIS subscription.

  {"country": "SG", "ports": ["SGSIN"], "partners": ["ID", "MY", ...]}

A file written for another country is ignored: a redeploy for a new country starts from the environment.
"""

import json
import os
from pathlib import Path
from typing import Any

FILE = "monitor_selection.json"


def load(data_dir: Path, country: str) -> dict[str, Any] | None:
    path = data_dir / FILE
    try:
        found = json.loads(path.read_text())
    except (OSError, ValueError):
        return None
    if not isinstance(found, dict) or found.get("country") != country:
        return None
    ports, partners = found.get("ports"), found.get("partners")
    if not (isinstance(ports, list) and isinstance(partners, list)):
        return None
    return {"country": country, "ports": [str(p) for p in ports], "partners": [str(c) for c in partners]}


def save(data_dir: Path, country: str, ports: list[str], partners: list[str]) -> None:
    data_dir.mkdir(parents=True, exist_ok=True)
    tmp = data_dir / (FILE + ".tmp")
    tmp.write_text(json.dumps({"country": country, "ports": ports, "partners": partners}))
    os.replace(tmp, data_dir / FILE)  # a reader sees the old file or the new one
