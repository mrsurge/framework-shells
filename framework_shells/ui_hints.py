import json
from pathlib import Path
from typing import cast

ManifestMap = dict[str, object]


def load_ui_hints(apps_dir: str | Path) -> dict[str, object]:
    """Load framework shell UI hints from app manifests in a directory."""
    apps_dir = Path(apps_dir)
    out: dict[str, object] = {}
    
    if not apps_dir.exists():
        return out
        
    for entry in apps_dir.iterdir():
        if not entry.is_dir():
            continue
            
        manifest_path = entry / "manifest.json"
        if not manifest_path.exists():
            continue
            
        try:
            with open(manifest_path, "r", encoding="utf-8") as fh:
                loaded = cast(object, json.load(fh))
        except Exception:
            continue

        if not isinstance(loaded, dict):
            continue
        manifest = cast(ManifestMap, loaded)
        app_id = manifest.get("id")
        ui = manifest.get("framework_shell_ui")
        resolved_app_id = str(app_id) if app_id else entry.name
        
        if isinstance(ui, dict) and ui:
            out[resolved_app_id] = cast(object, ui)
            
    return out
