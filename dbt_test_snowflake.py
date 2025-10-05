#!/usr/bin/env python3
"""
Automated DBT Unit Tests Generator for Snowflake
Generates comprehensive unit tests for dbt models including:
- Not null checks
- Unique constraints
- Relationship tests
- Accepted values
- Data type validations
- Custom business rule tests
"""

import os
import yaml
import json
import argparse
from pathlib import Path
from typing import Dict, List, Any, Optional
import re


class DBTTestGenerator:
    """Main class for generating dbt unit tests"""
    
    def __init__(self, project_path: str):
        self.project_path = Path(project_path)
        self.models_path = self.project_path / "models"
        self.tests_path = self.project_path / "tests"
        self.schema_path = self.models_path
        
    def parse_sql_model(self, model_path: Path) -> Dict[str, Any]:
        """Parse SQL model to extract columns and metadata"""
        with open(model_path, 'r') as f:
            content = f.read()
        
        # Extract column names from SELECT statement
        columns = []
        
        # Find SELECT clause
        select_match = re.search(r'SELECT\s+(.*?)\s+FROM', content, re.IGNORECASE | re.DOTALL)
        if select_match:
            select_clause = select_match.group(1)
            
            # Parse columns (simplified - handles basic cases)
            col_lines = select_clause.split(',')
            for col in col_lines:
                col = col.strip()
                # Extract alias if exists
                alias_match = re.search(r'AS\s+(\w+)', col, re.IGNORECASE)
                if alias_match:
                    columns.append(alias_match.group(1))
                else:
                    # Extract column name without functions
                    col_match = re.search(r'(\w+)(?:\s|$)', col)
                    if col_match:
                        columns.append(col_match.group(1))
        
        # Extract config if exists
        config = {}
        config_match = re.search(r'{{.*?config\((.*?)\).*?}}', content, re.DOTALL)
        if config_match:
            config_str = config_match.group(1)
            # Parse materialized type
            mat_match = re.search(r"materialized\s*=\s*['\"](\w+)['\"]", config_str)
            if mat_match:
                config['materialized'] = mat_match.group(1)
        
        return {
            'columns': columns,
            'config': config,
            'content': content
        }
    
    def detect_key_columns(self, columns: List[str]) -> Dict[str, List[str]]:
        """Detect primary keys, foreign keys, and important columns based on naming conventions"""
        key_info = {
            'primary_keys': [],
            'foreign_keys': [],
            'date_columns': [],
            'amount_columns': [],
            'status_columns': []
        }
        
        for col in columns:
            col_lower = col.lower()
            
            # Primary keys
            if col_lower in ['id', 'pk'] or col_lower.endswith('_id') and not col_lower.startswith('fk_'):
                if col_lower in ['id', 'pk'] or re.match(r'^[a-z]+_id$', col_lower):
                    key_info['primary_keys'].append(col)
            
            # Foreign keys
            if col_lower.startswith('fk_') or (col_lower.endswith('_id') and col_lower != 'id'):
                key_info['foreign_keys'].append(col)
            
            # Date columns
            if any(date_word in col_lower for date_word in ['date', 'timestamp', 'created', 'updated', 'modified']):
                key_info['date_columns'].append(col)
            
            # Amount columns
            if any(amt_word in col_lower for amt_word in ['amount', 'price', 'cost', 'total', 'sum', 'revenue']):
                key_info['amount_columns'].append(col)
            
            # Status columns
            if any(status_word in col_lower for status_word in ['status', 'state', 'type', 'category']):
                key_info['status_columns'].append(col)
        
        return key_info
    
    def generate_schema_tests(self, model_name: str, columns: List[str], 
                             key_info: Dict[str, List[str]]) -> Dict[str, Any]:
        """Generate schema.yml tests for a model"""
        tests_config = {
            'version': 2,
            'models': [{
                'name': model_name,
                'description': f'Model for {model_name}',
                'columns': []
            }]
        }
        
        for col in columns:
            col_tests = {
                'name': col,
                'description': f'{col} column',
                'tests': []
            }
            
            # Primary key tests
            if col in key_info['primary_keys']:
                col_tests['tests'].extend([
                    'not_null',
                    'unique'
                ])
            
            # Foreign key tests (not_null only, relationships require table reference)
            elif col in key_info['foreign_keys']:
                col_tests['tests'].append('not_null')
                # Add relationship test template (needs manual configuration)
                col_tests['tests'].append({
                    'relationships': {
                        'to': 'ref("REFERENCE_TABLE")',
                        'field': 'id',
                        'config': {
                            'severity': 'warn'
                        }
                    }
                })
            
            # Date columns
            elif col in key_info['date_columns']:
                col_tests['tests'].append('not_null')
            
            # Amount columns
            elif col in key_info['amount_columns']:
                col_tests['tests'].extend([
                    'not_null',
                    {
                        'dbt_utils.expression_is_true': {
                            'expression': f'{col} >= 0',
                            'config': {
                                'severity': 'warn'
                            }
                        }
                    }
                ])
            
            # Status columns with accepted values
            elif col in key_info['status_columns']:
                col_tests['tests'].append('not_null')
                col_tests['tests'].append({
                    'accepted_values': {
                        'values': ['DEFINE_VALID_VALUES'],
                        'config': {
                            'severity': 'warn'
                        }
                    }
                })
            
            # Default: just not_null for other columns
            else:
                col_tests['tests'].append('not_null')
            
            tests_config['models'][0]['columns'].append(col_tests)
        
        return tests_config
    
    def generate_custom_tests(self, model_name: str, key_info: Dict[str, List[str]]) -> str:
        """Generate custom SQL tests for business logic validation"""
        tests = []
        
        # Row count test
        tests.append(f"""
-- Row count validation for {model_name}
-- tests/custom/{model_name}_row_count_test.sql
{{{{
    config(
        severity='warn'
    )
}}}}

SELECT COUNT(*) as row_count
FROM {{{{ ref('{model_name}') }}}}
HAVING row_count = 0  -- Fail if no rows
""")
        
        # Duplicate check for primary keys
        if key_info['primary_keys']:
            pk_cols = ', '.join(key_info['primary_keys'])
            tests.append(f"""
-- Duplicate check for {model_name}
-- tests/custom/{model_name}_duplicate_check.sql
{{{{
    config(
        severity='error'
    )
}}}}

SELECT {pk_cols}, COUNT(*) as cnt
FROM {{{{ ref('{model_name}') }}}}
GROUP BY {pk_cols}
HAVING COUNT(*) > 1  -- Fail if duplicates found
""")
        
        # Date range validation
        if key_info['date_columns']:
            date_col = key_info['date_columns'][0]
            tests.append(f"""
-- Date range validation for {model_name}
-- tests/custom/{model_name}_date_range_test.sql
{{{{
    config(
        severity='warn'
    )
}}}}

SELECT *
FROM {{{{ ref('{model_name}') }}}}
WHERE {date_col} > CURRENT_DATE()
   OR {date_col} < DATEADD(year, -10, CURRENT_DATE())  -- Adjust as needed
""")
        
        # Negative amount check
        if key_info['amount_columns']:
            tests.append(f"""
-- Negative amount check for {model_name}
-- tests/custom/{model_name}_amount_validation.sql
{{{{
    config(
        severity='warn'
    )
}}}}

SELECT *
FROM {{{{ ref('{model_name}') }}}}
WHERE {' < 0 OR '.join(key_info['amount_columns'])} < 0
""")
        
        # Null percentage check
        all_cols_check = '\n    OR '.join([f'{col} IS NULL' for col in key_info['primary_keys'] + key_info['foreign_keys']])
        if all_cols_check:
            tests.append(f"""
-- Null percentage check for critical columns in {model_name}
-- tests/custom/{model_name}_null_percentage_test.sql
{{{{
    config(
        severity='warn'
    )
}}}}

WITH null_counts AS (
    SELECT 
        COUNT(*) as total_rows,
        SUM(CASE WHEN {all_cols_check} THEN 1 ELSE 0 END) as null_rows
    FROM {{{{ ref('{model_name}') }}}}
)
SELECT *
FROM null_counts
WHERE (null_rows::FLOAT / NULLIF(total_rows, 0)) > 0.05  -- Fail if >5% nulls
""")
        
        return '\n'.join(tests)
    
    def generate_data_quality_tests(self, model_name: str) -> str:
        """Generate comprehensive data quality test suite"""
        return f"""
-- Comprehensive Data Quality Tests for {model_name}
-- tests/data_quality/{model_name}_quality_suite.sql

{{{{
    config(
        severity='warn',
        tags=['data_quality', '{model_name}']
    )
}}}}

-- Test 1: Check for completely null rows
WITH null_row_check AS (
    SELECT *
    FROM {{{{ ref('{model_name}') }}}}
    WHERE 1=0  -- Add conditions to check if all important columns are null
),

-- Test 2: Check for future dates
future_date_check AS (
    SELECT *
    FROM {{{{ ref('{model_name}') }}}}
    WHERE 1=0  -- Add date column checks
),

-- Test 3: Referential integrity check
ref_integrity_check AS (
    SELECT *
    FROM {{{{ ref('{model_name}') }}}}
    WHERE 1=0  -- Add foreign key validation
)

-- Combine all test results
SELECT * FROM null_row_check
UNION ALL
SELECT * FROM future_date_check
UNION ALL
SELECT * FROM ref_integrity_check
"""
    
    def save_tests(self, model_name: str, schema_tests: Dict, custom_tests: str, 
                   data_quality_tests: str):
        """Save generated tests to appropriate directories"""
        # Create test directories
        schema_dir = self.models_path / Path(model_name).parent
        custom_dir = self.tests_path / "custom"
        quality_dir = self.tests_path / "data_quality"
        
        custom_dir.mkdir(parents=True, exist_ok=True)
        quality_dir.mkdir(parents=True, exist_ok=True)
        
        # Save schema tests
        schema_file = schema_dir / f"schema_{model_name}.yml"
        with open(schema_file, 'w') as f:
            yaml.dump(schema_tests, f, default_flow_style=False, sort_keys=False)
        print(f"✓ Generated schema tests: {schema_file}")
        
        # Save custom tests
        custom_file = custom_dir / f"{model_name}_custom_tests.sql"
        with open(custom_file, 'w') as f:
            f.write(custom_tests)
        print(f"✓ Generated custom tests: {custom_file}")
        
        # Save data quality tests
        quality_file = quality_dir / f"{model_name}_quality_tests.sql"
        with open(quality_file, 'w') as f:
            f.write(data_quality_tests)
        print(f"✓ Generated data quality tests: {quality_file}")
    
    def generate_tests_for_model(self, model_path: Path):
        """Generate all tests for a single model"""
        model_name = model_path.stem
        print(f"\n{'='*60}")
        print(f"Generating tests for model: {model_name}")
        print(f"{'='*60}")
        
        # Parse model
        model_info = self.parse_sql_model(model_path)
        columns = model_info['columns']
        
        if not columns:
            print(f"⚠ Warning: No columns detected in {model_name}. Skipping.")
            return
        
        print(f"Detected {len(columns)} columns: {', '.join(columns)}")
        
        # Detect key columns
        key_info = self.detect_key_columns(columns)
        print(f"Key columns identified:")
        print(f"  - Primary keys: {key_info['primary_keys']}")
        print(f"  - Foreign keys: {key_info['foreign_keys']}")
        print(f"  - Date columns: {key_info['date_columns']}")
        print(f"  - Amount columns: {key_info['amount_columns']}")
        print(f"  - Status columns: {key_info['status_columns']}")
        
        # Generate tests
        schema_tests = self.generate_schema_tests(model_name, columns, key_info)
        custom_tests = self.generate_custom_tests(model_name, key_info)
        data_quality_tests = self.generate_data_quality_tests(model_name)
        
        # Save tests
        self.save_tests(model_name, schema_tests, custom_tests, data_quality_tests)
    
    def generate_all_tests(self, model_pattern: str = "*.sql"):
        """Generate tests for all models matching the pattern"""
        model_files = list(self.models_path.rglob(model_pattern))
        
        if not model_files:
            print(f"No models found in {self.models_path}")
            return
        
        print(f"\nFound {len(model_files)} models to process")
        
        for model_file in model_files:
            try:
                self.generate_tests_for_model(model_file)
            except Exception as e:
                print(f"✗ Error processing {model_file}: {str(e)}")
        
        print(f"\n{'='*60}")
        print(f"Test generation complete!")
        print(f"{'='*60}")
        print(f"\nNext steps:")
        print(f"1. Review generated schema.yml files and update REFERENCE_TABLE placeholders")
        print(f"2. Update accepted_values with actual valid values")
        print(f"3. Customize business logic tests as needed")
        print(f"4. Run: dbt test --select {model_pattern.replace('.sql', '')}")


def main():
    parser = argparse.ArgumentParser(
        description='Automated DBT Unit Tests Generator for Snowflake'
    )
    parser.add_argument(
        'project_path',
        help='Path to dbt project directory'
    )
    parser.add_argument(
        '--model',
        help='Generate tests for specific model (without .sql extension)',
        default=None
    )
    parser.add_argument(
        '--pattern',
        help='Model file pattern (default: *.sql)',
        default='*.sql'
    )
    
    args = parser.parse_args()
    
    # Initialize generator
    generator = DBTTestGenerator(args.project_path)
    
    # Generate tests
    if args.model:
        model_path = generator.models_path / f"{args.model}.sql"
        if model_path.exists():
            generator.generate_tests_for_model(model_path)
        else:
            print(f"Error: Model file not found: {model_path}")
    else:
        generator.generate_all_tests(args.pattern)


if __name__ == "__main__":
    main()
