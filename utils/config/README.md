# Configuration Module

This module handles the configuration for the dbxdwrecon project.

## Files

- `config.yml` - Main configuration file with Databricks and Oracle connection settings.
- `config.py` - Utilities to load configuration files.
- `datatype_mappings.yml` - Mappings between Oracle and Databricks data types.

## Data Type Mappings

The `datatype_mappings.yml` file contains:

1. Simple mappings from Oracle to Databricks data types.
2. Special rules for Oracle NUMBER types with different precision and scale.

These mappings are used to:
- Create a Databricks table containing all mappings
- Map Oracle metadata to equivalent Databricks data types during reconciliation
- Support schema comparison between Oracle and Databricks

The mappings are stored in two Databricks tables:
- `oracle_to_databricks_type_mappings` - Simple type mappings
- `oracle_to_databricks_type_mappings_rules` - Rules for NUMBER types

## Usage

Import the `DatatypeMapper` class to use these mappings:

```python
from utils.datatype_mapper import DatatypeMapper

# Initialize the mapper
mapper = DatatypeMapper()

# Create the mappings table in Databricks
mapper.create_mappings_table()

# Map an Oracle type to a Databricks type
databricks_type = mapper.map_oracle_to_databricks_type(
    "NUMBER", 
    precision=10, 
    scale=2
)
```

## Extending the Mappings

To add new mappings, simply edit the `datatype_mappings.yml` file and add entries to either:
- The `oracle_to_databricks` section for simple mappings.
- The `number_precision_rules` section for precision/scale-specific rules.

After updating the file, run the `create_mappings_table()` method to update the Databricks table. 