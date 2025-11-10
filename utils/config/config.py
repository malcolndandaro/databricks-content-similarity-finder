from typing import Dict, Any
import yaml
from pathlib import Path

def load_config() -> Dict[str, Any]:
    """
    Load configuration from config.yml file.
    Returns:
        Dict[str, Any]: Configuration dictionary
    """
    config_path = Path(__file__).parent / "config.yml"
    with open(config_path) as f:
        return yaml.safe_load(f)
