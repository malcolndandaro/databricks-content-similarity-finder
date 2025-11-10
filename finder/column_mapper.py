"""
Column mapping utilities for the finder module.

This module provides functionality to load and manage column mappings
between source and target tables from CSV configuration files.
"""

import csv
from typing import Dict, List, Optional, Tuple
from pathlib import Path
import logging

class ColumnMapper:
    """Utility class for managing column mappings between tables."""
    
    def __init__(self, mapping_file_path: Optional[str] = None):
        """
        Initialize the ColumnMapper.
        
        Args:
            mapping_file_path: Path to the CSV mapping file. If None, no mappings are loaded.
        """
        self.logger = logging.getLogger(__name__)
        self.mappings: Dict[str, Dict[str, List[Dict[str, str]]]] = {}
        
        if mapping_file_path:
            self.load_mappings(mapping_file_path)
    
    def _construct_table_name(self, catalog: str, schema: str, table: str) -> str:
        """
        Construct full table name from catalog, schema, and table.
        
        Args:
            catalog: Catalog name (can be empty/null for Oracle)
            schema: Schema name
            table: Table name
        
        Returns:
            Constructed table name in format catalog.schema.table or schema.table if catalog is null
        """
        if catalog and catalog.strip():
            return f"{catalog}.{schema}.{table}"
        else:
            return f"{schema}.{table}"
    
    def load_mappings(self, mapping_file_path: str) -> None:
        """
        Load column mappings from a CSV file.
        
        Expected CSV columns:
        - source_catalog
        - source_schema  
        - source_table
        - source_column
        - target_catalog
        - target_schema
        - target_table
        - target_column
        
        Additional columns are ignored.
        
        Args:
            mapping_file_path: Path to the CSV mapping file
        
        Raises:
            FileNotFoundError: If the mapping file doesn't exist
            ValueError: If the CSV file doesn't have the required columns
        """
        mapping_path = Path(mapping_file_path)
        
        if not mapping_path.exists():
            raise FileNotFoundError(f"Mapping file not found: {mapping_file_path}")
        
        try:
            with open(mapping_path, 'r', encoding='utf-8') as file:
                reader = csv.DictReader(file)
                
                # Check for required columns (case-insensitive)
                required_columns = {
                    'source_catalog', 'source_schema', 'source_table', 'source_column',
                    'target_catalog', 'target_schema', 'target_table', 'target_column'
                }
                
                # Create a mapping from lowercase header names to actual header names
                header_mapping = {header.lower(): header for header in reader.fieldnames}
                
                # Check if all required columns are present
                missing_columns = required_columns - set(header_mapping.keys())
                if missing_columns:
                    raise ValueError(f"Missing required columns in CSV: {missing_columns}")
                
                # Get the actual header names to use
                source_catalog_col = header_mapping['source_catalog']
                source_schema_col = header_mapping['source_schema']
                source_table_col = header_mapping['source_table']
                source_column_col = header_mapping['source_column']
                target_catalog_col = header_mapping['target_catalog']
                target_schema_col = header_mapping['target_schema']
                target_table_col = header_mapping['target_table']
                target_column_col = header_mapping['target_column']
                
                mapping_count = 0
                for row in reader:
                    # Skip empty rows
                    if not any(row.values()):
                        continue
                    
                    # Extract values
                    source_catalog = row[source_catalog_col] if row[source_catalog_col] else ""
                    source_schema = row[source_schema_col].strip()
                    source_table = row[source_table_col].strip()
                    source_column = row[source_column_col].strip()
                    target_catalog = row[target_catalog_col] if row[target_catalog_col] else ""
                    target_schema = row[target_schema_col].strip()
                    target_table = row[target_table_col].strip()
                    target_column = row[target_column_col].strip()
                    
                    # Skip rows with missing essential data
                    if not source_schema or not source_table or not source_column or \
                       not target_schema or not target_table or not target_column:
                        self.logger.warning(f"Skipping row with missing data: {row}")
                        continue
                    
                    # Construct full table names
                    source_table_full = self._construct_table_name(source_catalog, source_schema, source_table)
                    target_table_full = self._construct_table_name(target_catalog, target_schema, target_table)
                    
                    # Add to mappings structure
                    if source_table_full not in self.mappings:
                        self.mappings[source_table_full] = {}
                    
                    if target_table_full not in self.mappings[source_table_full]:
                        self.mappings[source_table_full][target_table_full] = []
                    
                    # Add column mapping
                    self.mappings[source_table_full][target_table_full].append({
                        'source': source_column,
                        'target': target_column
                    })
                    
                    mapping_count += 1
                
                self.logger.info(f"Loaded {mapping_count} column mappings from CSV")
                
                # Log summary of loaded mappings
                for source_table, target_tables in self.mappings.items():
                    for target_table, column_mappings in target_tables.items():
                        self.logger.info(f"  {source_table} -> {target_table}: {len(column_mappings)} column mappings")
                        
        except (csv.Error, KeyError, ValueError) as e:
            raise ValueError(f"Error parsing CSV file {mapping_file_path}: {e}")
    
    def get_column_mappings(self, source_table: str, target_table: str) -> Optional[List[Tuple[str, str]]]:
        """
        Get column mappings for a specific source-target table pair.
        
        Args:
            source_table: Name of the source table
            target_table: Name of the target table
        
        Returns:
            List of tuples (source_column, target_column) if mappings exist, None otherwise
        """
        if source_table in self.mappings:
            if target_table in self.mappings[source_table]:
                column_mappings = self.mappings[source_table][target_table]
                return [(mapping['source'].upper(), mapping['target'].upper()) 
                       for mapping in column_mappings]
        
        return None
    
    def has_mappings(self, source_table: str, target_table: str) -> bool:
        """
        Check if column mappings exist for a specific source-target table pair.
        
        Args:
            source_table: Name of the source table
            target_table: Name of the target table
        
        Returns:
            True if mappings exist, False otherwise
        """
        return self.get_column_mappings(source_table, target_table) is not None
    
    def get_all_source_tables(self) -> List[str]:
        """Get all source tables that have mappings defined."""
        return list(self.mappings.keys())
    
    def get_target_tables(self, source_table: str) -> List[str]:
        """
        Get all target tables for a specific source table.
        
        Args:
            source_table: Name of the source table
        
        Returns:
            List of target table names
        """
        return list(self.mappings.get(source_table, {}).keys())

def create_example_mapping_file(file_path: str) -> None:
    """
    Create an example column mapping CSV file.
    
    Args:
        file_path: Path where the example file should be created
    """
    example_data = [
        {
            'source_catalog': '',
            'source_schema': 'BCP',
            'source_table': 'oracle_table_b',
            'source_column': 'ID',
            'target_catalog': 'malcoln',
            'target_schema': 'bcp',
            'target_table': 'oracle_table_b_subset_2',
            'target_column': 'ID',
            'notes': 'Primary key mapping'
        },
        {
            'source_catalog': '',
            'source_schema': 'BCP',
            'source_table': 'oracle_table_b',
            'source_column': 'uuid',
            'target_catalog': 'malcoln',
            'target_schema': 'bcp',
            'target_table': 'oracle_table_b_subset_2',
            'target_column': 'uuid',
            'notes': 'UUID mapping'
        },
        {
            'source_catalog': '',
            'source_schema': 'BCP',
            'source_table': 'oracle_table_b',
            'source_column': 'IP_ADDRESS',
            'target_catalog': 'malcoln',
            'target_schema': 'bcp',
            'target_table': 'oracle_table_b_subset_2',
            'target_column': 'IP_ADDRESS',
            'notes': 'IP address field'
        },
        {
            'source_catalog': '',
            'source_schema': 'BCP',
            'source_table': 'oracle_table_b',
            'source_column': 'NU_TLFN',
            'target_catalog': 'malcoln',
            'target_schema': 'bcp',
            'target_table': 'oracle_table_b_subset_2',
            'target_column': 'NU_TLFN',
            'notes': 'Phone number field'
        },
        {
            'source_catalog': '',
            'source_schema': 'BCP',
            'source_table': 'oracle_table_b',
            'source_column': 'nu_cep',
            'target_catalog': 'malcoln',
            'target_schema': 'bcp',
            'target_table': 'oracle_table_b_subset_2',
            'target_column': 'nu_cep',
            'notes': 'Postal code field'
        }
    ]
    
    with open(file_path, 'w', newline='', encoding='utf-8') as file:
        fieldnames = ['source_catalog', 'source_schema', 'source_table', 'source_column', 
                     'target_catalog', 'target_schema', 'target_table', 'target_column', 'notes']
        writer = csv.DictWriter(file, fieldnames=fieldnames)
        
        writer.writeheader()
        writer.writerows(example_data)
    
    print(f"Example mapping CSV file created at: {file_path}") 