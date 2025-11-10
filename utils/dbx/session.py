from typing import Optional
import os
from databricks.connect import DatabricksSession
from ..config.config import load_config

def initialize_dbx_session() -> DatabricksSession:
    """
    Initialize a Databricks session based on the environment.
    If running in Databricks, returns the existing session.
    If running locally, creates a new session using configuration.
    
    Returns:
        DatabricksSession: The initialized Databricks session
    """
    # Check if running in Databricks environment
    is_databricks = os.environ.get("DATABRICKS_RUNTIME_VERSION") is not None

    if is_databricks:
        # In Databricks environment, the session is already available
        return DatabricksSession.builder.getOrCreate()
    else:
        # Use DatabricksSession if running locally
        config = load_config()
        return DatabricksSession.builder.clusterId(config["databricks"]["cluster_id"]).getOrCreate()

# Alias for consistency with new module name


