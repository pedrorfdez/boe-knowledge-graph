from pathlib import Path
import yaml
from pipeline.adapters.models import SourceAdapter


def load_canonical(canonical_path: Path) -> dict[str, list[str]]:
    with open(canonical_path) as f:
        return yaml.safe_load(f)


def load_adapter(adapter_path: Path, canonical_path: Path) -> SourceAdapter:
    canonical = load_canonical(canonical_path)
    with open(adapter_path) as f:
        data = yaml.safe_load(f)
    data["_canonical"] = canonical
    return SourceAdapter(**data)
