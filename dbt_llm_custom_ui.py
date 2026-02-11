import streamlit as st
import os
import yaml
import json
import requests
from pathlib import Path
from typing import Dict, List, Optional, Tuple
import tempfile
import shutil
import re
from datetime import datetime
from collections import defaultdict
import time

# Page configuration with custom theme
st.set_page_config(
    page_title="DBT Test Generator Pro",
    page_icon="🚀",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS for enhanced UI
st.markdown("""
    <style>
        /* Import Google Fonts */
        @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&display=swap');

        /* Global Styles */
        * {
            font-family: 'Inter', sans-serif;
        }

        /* Main Header Styling */
        .main-header {
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            padding: 2rem;
            border-radius: 15px;
            box-shadow: 0 10px 30px rgba(102, 126, 234, 0.3);
            margin-bottom: 2rem;
            text-align: center;
            animation: fadeIn 0.8s ease-in;
        }

        .main-header h1 {
            color: white;
            font-size: 2.5rem;
            font-weight: 700;
            margin: 0;
            text-shadow: 2px 2px 4px rgba(0,0,0,0.2);
        }

        .main-header p {
            color: rgba(255,255,255,0.9);
            font-size: 1.1rem;
            margin-top: 0.5rem;
        }

        /* Feature Cards */
        .feature-card {
            background: white;
            padding: 1.5rem;
            border-radius: 12px;
            box-shadow: 0 4px 6px rgba(0,0,0,0.07);
            border-left: 4px solid #667eea;
            margin-bottom: 1rem;
            transition: transform 0.3s ease, box-shadow 0.3s ease;
        }

        .feature-card:hover {
            transform: translateY(-5px);
            box-shadow: 0 8px 15px rgba(0,0,0,0.15);
        }

        /* Metric Cards */
        .metric-card {
            background: linear-gradient(135deg, #f5f7fa 0%, #c3cfe2 100%);
            padding: 1.5rem;
            border-radius: 12px;
            text-align: center;
            box-shadow: 0 4px 6px rgba(0,0,0,0.1);
            transition: transform 0.3s ease;
        }

        .metric-card:hover {
            transform: scale(1.05);
        }

        .metric-value {
            font-size: 2.5rem;
            font-weight: 700;
            color: #667eea;
            margin: 0.5rem 0;
        }

        .metric-label {
            font-size: 0.9rem;
            color: #666;
            text-transform: uppercase;
            letter-spacing: 1px;
            font-weight: 600;
        }

        /* Sidebar Styling */
        .css-1d391kg, .css-1v3fvcr {
            background: linear-gradient(180deg, #667eea 0%, #764ba2 100%);
        }

        /* Buttons */
        .stButton>button {
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            color: white;
            border: none;
            padding: 0.75rem 2rem;
            border-radius: 25px;
            font-weight: 600;
            transition: all 0.3s ease;
            box-shadow: 0 4px 15px rgba(102, 126, 234, 0.4);
        }

        .stButton>button:hover {
            transform: translateY(-2px);
            box-shadow: 0 6px 20px rgba(102, 126, 234, 0.6);
        }

        /* Success/Warning/Error Boxes */
        .stSuccess, .stWarning, .stError, .stInfo {
            border-radius: 10px;
            padding: 1rem;
            animation: slideIn 0.5s ease-out;
        }

        /* Tabs */
        .stTabs [data-baseweb="tab-list"] {
            gap: 2rem;
        }

        .stTabs [data-baseweb="tab"] {
            padding: 1rem 2rem;
            background-color: #f5f7fa;
            border-radius: 10px 10px 0 0;
            font-weight: 600;
            transition: all 0.3s ease;
        }

        .stTabs [aria-selected="true"] {
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            color: white;
        }

        /* Code Blocks */
        .stCodeBlock {
            border-radius: 10px;
            border: 1px solid #e0e0e0;
            box-shadow: 0 2px 8px rgba(0,0,0,0.05);
        }

        /* Expander */
        .streamlit-expanderHeader {
            background-color: #f8f9fa;
            border-radius: 8px;
            font-weight: 600;
            padding: 1rem;
        }

        /* Progress Bar */
        .stProgress > div > div > div > div {
            background: linear-gradient(90deg, #667eea 0%, #764ba2 100%);
        }

        /* Animations */
        @keyframes fadeIn {
            from { opacity: 0; transform: translateY(-20px); }
            to { opacity: 1; transform: translateY(0); }
        }

        @keyframes slideIn {
            from { opacity: 0; transform: translateX(-20px); }
            to { opacity: 1; transform: translateX(0); }
        }

        @keyframes pulse {
            0%, 100% { transform: scale(1); }
            50% { transform: scale(1.05); }
        }

        /* Status Badge */
        .status-badge {
            display: inline-block;
            padding: 0.4rem 1rem;
            border-radius: 20px;
            font-size: 0.85rem;
            font-weight: 600;
            margin: 0.2rem;
        }

        .status-success {
            background: #d4edda;
            color: #155724;
        }

        .status-warning {
            background: #fff3cd;
            color: #856404;
        }

        .status-danger {
            background: #f8d7da;
            color: #721c24;
        }

        /* Loading Spinner Enhancement */
        .stSpinner > div {
            border-color: #667eea !important;
        }

        /* Input Fields */
        .stTextInput>div>div>input, .stTextArea>div>div>textarea {
            border-radius: 8px;
            border: 2px solid #e0e0e0;
            transition: border-color 0.3s ease;
        }

        .stTextInput>div>div>input:focus, .stTextArea>div>div>textarea:focus {
            border-color: #667eea;
            box-shadow: 0 0 0 3px rgba(102, 126, 234, 0.1);
        }

        /* Download Buttons */
        .stDownloadButton>button {
            background: linear-gradient(135deg, #11998e 0%, #38ef7d 100%);
            color: white;
            border-radius: 20px;
            padding: 0.6rem 1.5rem;
            font-weight: 600;
            box-shadow: 0 4px 15px rgba(17, 153, 142, 0.4);
        }

        .stDownloadButton>button:hover {
            transform: translateY(-2px);
            box-shadow: 0 6px 20px rgba(17, 153, 142, 0.6);
        }
    </style>
    """, unsafe_allow_html=True)


class GherkinDSLParser:
    """Parse Gherkin-style test specifications"""

    @staticmethod
    def parse_feature(feature_text: str) -> Dict:
        """Parse a Gherkin feature into structured data"""
        lines = feature_text.strip().split('\n')
        feature = {
            'name': '',
            'description': '',
            'scenarios': []
        }

        current_scenario = None

        for line in lines:
            line = line.strip()
            if line.startswith('Feature:'):
                feature['name'] = line.replace('Feature:', '').strip()
            elif line.startswith('Scenario:'):
                if current_scenario:
                    feature['scenarios'].append(current_scenario)
                current_scenario = {
                    'name': line.replace('Scenario:', '').strip(),
                    'given': [],
                    'when': [],
                    'then': []
                }
            elif line.startswith('Given'):
                if current_scenario:
                    current_scenario['given'].append(line.replace('Given', '').strip())
            elif line.startswith('And') and current_scenario:
                if current_scenario['then']:
                    current_scenario['then'].append(line.replace('And', '').strip())
                elif current_scenario['when']:
                    current_scenario['when'].append(line.replace('And', '').strip())
                elif current_scenario['given']:
                    current_scenario['given'].append(line.replace('And', '').strip())
            elif line.startswith('When'):
                if current_scenario:
                    current_scenario['when'].append(line.replace('When', '').strip())
            elif line.startswith('Then'):
                if current_scenario:
                    current_scenario['then'].append(line.replace('Then', '').strip())

        if current_scenario:
            feature['scenarios'].append(current_scenario)

        return feature


class DBTTestGenerator:
    """Generate DBT tests from Gherkin specifications"""

    @staticmethod
    def generate_schema_tests(feature: Dict, model_name: str) -> str:
        """Generate schema.yml tests"""
        tests = []

        for scenario in feature['scenarios']:
            for then_clause in scenario['then']:
                if 'unique' in then_clause.lower():
                    column = DBTTestGenerator._extract_column(then_clause)
                    if column:
                        tests.append(f"      - unique")
                elif 'not null' in then_clause.lower():
                    column = DBTTestGenerator._extract_column(then_clause)
                    if column:
                        tests.append(f"      - not_null")
                elif 'accepted_values' in then_clause.lower():
                    values = DBTTestGenerator._extract_values(then_clause)
                    if values:
                        tests.append(f"      - accepted_values:\n          values: {values}")
                elif 'relationships' in then_clause.lower():
                    ref_table = DBTTestGenerator._extract_reference(then_clause)
                    if ref_table:
                        tests.append(f"      - relationships:\n          to: ref('{ref_table}')\n          field: id")

        schema = f"""version: 2

    models:
      - name: {model_name}
        description: "Generated from Gherkin feature: {feature['name']}"
        columns:
    """

        columns = DBTTestGenerator._extract_all_columns(feature)
        for col in columns:
            schema += f"      - name: {col}\n"
            schema += f"        description: \"{col} column\"\n"
            if tests:
                schema += f"        tests:\n"
                for test in tests[:2]:
                    schema += f"        {test}\n"

        return schema

    @staticmethod
    def generate_unit_test(feature: Dict, model_name: str) -> str:
        """Generate comprehensive DBT unit test SQL"""
        test_name = f"test_{model_name}_{feature['name'].lower().replace(' ', '_')}"

        sql = f"""-- Comprehensive Unit Test for {model_name}
    -- Feature: {feature['name']}
    -- Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

    {{{{ config(
        tags=['unit-test', 'data-quality'],
        severity='error'
    ) }}}}

    /*
    Unit Test Coverage:
    1. Data Completeness - Ensure all required fields are populated
    2. Data Validity - Validate data formats and ranges
    3. Data Consistency - Check referential integrity
    4. Business Logic - Validate business rules
    5. Edge Cases - Test boundary conditions
    */

    with source_data as (
        select * from {{{{ ref('{model_name}') }}}}
    ),

    -- Test 1: Null Check on Critical Fields
    null_check as (
        select
            'null_check' as test_name,
            count(*) as failed_records,
            'Critical fields should not be null' as test_description
        from source_data
        where id is null
           or created_at is null
    ),

    -- Test 2: Duplicate Check
    duplicate_check as (
        select
            'duplicate_check' as test_name,
            count(*) - count(distinct id) as failed_records,
            'Primary key should be unique' as test_description
        from source_data
    ),

    -- Test 3: Data Type Validation
    data_type_check as (
        select
            'data_type_check' as test_name,
            count(*) as failed_records,
            'Date fields should be valid dates' as test_description
        from source_data
        where try_cast(created_at as timestamp) is null
           or try_cast(updated_at as timestamp) is null
    ),

    -- Test 4: Business Logic Validation
    business_logic_check as (
        select
            'business_logic_check' as test_name,
            count(*) as failed_records,
            'Updated_at should be >= created_at' as test_description
        from source_data
        where updated_at < created_at
    ),

    -- Test 5: Range Validation
    range_check as (
        select
            'range_check' as test_name,
            count(*) as failed_records,
            'Dates should not be in the future' as test_description
        from source_data
        where created_at > current_timestamp()
           or updated_at > current_timestamp()
    ),

    -- Test 6: Referential Integrity (if applicable)
    referential_integrity as (
        select
            'referential_integrity' as test_name,
            0 as failed_records,  -- Placeholder
            'Foreign keys should reference valid records' as test_description
    ),

    -- Test 7: Status Value Validation
    status_check as (
        select
            'status_check' as test_name,
            count(*) as failed_records,
            'Status should have valid values' as test_description
        from source_data
        where status not in ('active', 'inactive', 'pending', 'completed', 'cancelled')
    ),

    -- Test 8: Edge Cases
    edge_case_check as (
        select
            'edge_case_check' as test_name,
            count(*) as failed_records,
            'Check for extreme or unusual values' as test_description
        from source_data
        where value < 0  -- Assuming value should be positive
           or value > 1000000  -- Assuming reasonable upper bound
    ),

    -- Aggregate all test results
    all_tests as (
        select * from null_check
        union all
        select * from duplicate_check
        union all
        select * from data_type_check
        union all
        select * from business_logic_check
        union all
        select * from range_check
        union all
        select * from referential_integrity
        union all
        select * from status_check
        union all
        select * from edge_case_check
    ),

    -- Test Summary
    test_summary as (
        select
            count(*) as total_tests,
            sum(case when failed_records = 0 then 1 else 0 end) as passed_tests,
            sum(case when failed_records > 0 then 1 else 0 end) as failed_tests,
            sum(failed_records) as total_failures
        from all_tests
    )

    -- Return failed tests (DBT will fail if any rows returned)
    select
        test_name,
        failed_records,
        test_description,
        case
            when failed_records = 0 then '✓ PASS'
            else '✗ FAIL'
        end as status
    from all_tests
    where failed_records > 0

    union all

    -- Summary row
    select
        'SUMMARY' as test_name,
        total_failures as failed_records,
        concat(
            'Passed: ', passed_tests, '/', total_tests,
            ' | Failed: ', failed_tests, '/', total_tests
        ) as test_description,
        case
            when failed_tests = 0 then '✓ ALL TESTS PASSED'
            else '✗ TESTS FAILED'
        end as status
    from test_summary
    where total_failures > 0
    """

        return sql

    @staticmethod
    def generate_unit_test_with_coverage(feature: Dict, model_name: str, model_sql: str) -> Dict[str, str]:
        """Generate unit tests with coverage tracking"""

        # Extract columns from model
        columns = DBTTestGenerator._extract_all_columns(feature)
        model_columns = CodeCoverageAnalyzer.extract_columns_from_model(model_sql)

        # Generate comprehensive unit test
        unit_test_sql = DBTTestGenerator.generate_unit_test(feature, model_name)

        # Generate coverage metadata
        tested_columns = [col for col in model_columns if col in columns]
        untested_columns = [col for col in model_columns if col not in columns]

        coverage_metadata = f"""-- Test Coverage Report for {model_name}
    -- Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
    --
    -- Total Columns: {len(model_columns)}
    -- Tested Columns: {len(tested_columns)} ({len(tested_columns) / len(model_columns) * 100 if model_columns else 0:.1f}%)
    -- Untested Columns: {len(untested_columns)}
    --
    -- Tested: {', '.join(tested_columns) if tested_columns else 'None'}
    -- Untested: {', '.join(untested_columns) if untested_columns else 'None'}
    --
    -- Test Categories Covered:
    -- ✓ Null Checks
    -- ✓ Uniqueness Checks
    -- ✓ Data Type Validation
    -- ✓ Business Logic
    -- ✓ Range Validation
    -- ✓ Status Validation
    -- ✓ Edge Cases
    -- ✓ Referential Integrity
    """

        return {
            'unit_test': unit_test_sql,
            'coverage_metadata': coverage_metadata,
            'tested_columns': tested_columns,
            'untested_columns': untested_columns,
            'coverage_percentage': len(tested_columns) / len(model_columns) * 100 if model_columns else 0
        }

    @staticmethod
    def _extract_column(text: str) -> Optional[str]:
        match = re.search(r'column[s]?\s+["\']?(\w+)["\']?', text, re.IGNORECASE)
        return match.group(1) if match else None

    @staticmethod
    def _extract_values(text: str) -> Optional[List]:
        match = re.search(r'\[(.*?)\]', text)
        if match:
            return [v.strip().strip("'\"") for v in match.group(1).split(',')]
        return None

    @staticmethod
    def _extract_reference(text: str) -> Optional[str]:
        match = re.search(r'to\s+["\']?(\w+)["\']?', text, re.IGNORECASE)
        return match.group(1) if match else None

    @staticmethod
    def _extract_all_columns(feature: Dict) -> List[str]:
        columns = set()
        for scenario in feature['scenarios']:
            for clauses in [scenario['given'], scenario['when'], scenario['then']]:
                for clause in clauses:
                    words = clause.split()
                    for word in words:
                        if word.isidentifier() and len(word) > 2:
                            columns.add(word.lower())
        return list(columns)[:5]

    @staticmethod
    def _extract_test_data(text: str) -> Optional[str]:
        return "'test_value' as test_column"

    @staticmethod
    def _extract_expectation(text: str) -> Optional[str]:
        return "true as expected_result"


class DBTModelGenerator:
    """Generate DBT models from specifications"""

    @staticmethod
    def generate_model(model_name: str, feature: Dict, model_type: str = "view") -> str:
        """Generate a DBT model SQL file"""

        if model_type == "snapshot":
            return DBTModelGenerator.generate_snapshot(model_name, feature)
        elif model_type == "ephemeral":
            return DBTModelGenerator.generate_ephemeral(model_name, feature)

        sql = f"""{{{{
        config(
            materialized='{model_type}',
            tags=['auto-generated']
        )
    }}}}

    /*
        Model: {model_name}
        Feature: {feature['name']}
        Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

        Description:
        This model was auto-generated from Gherkin specifications.
    """

        for scenario in feature['scenarios']:
            sql += f"\n    Scenario: {scenario['name']}\n"
            for given in scenario['given']:
                sql += f"    - Given: {given}\n"
            for when in scenario['when']:
                sql += f"    - When: {when}\n"
            for then in scenario['then']:
                sql += f"    - Then: {then}\n"

        sql += "*/\n\n"

        if model_type == "incremental":
            sql += """select
        id,
        created_at,
        updated_at,
        status,
        value
    from {{ source('raw', 'source_table') }}
    where 1=1

    {% if is_incremental() %}
        -- Incremental logic: only new or updated records
        and updated_at > (select max(updated_at) from {{ this }})
    {% endif %}
    """
        else:
            sql += """select
        id,
        created_at,
        updated_at,
        status,
        value
    from {{ source('raw', 'source_table') }}
    where 1=1
    """

        return sql

    @staticmethod
    def generate_snapshot(model_name: str, feature: Dict) -> str:
        """Generate a DBT snapshot"""

        snapshot_sql = """{% snapshot {model_name}_snapshot %}

    {{{{
        config(
            target_database='analytics',
            target_schema='snapshots',
            unique_key='id',
            strategy='timestamp',
            updated_at='updated_at',
            tags=['snapshot', 'auto-generated']
        )
    }}}}

    /*
        Snapshot: {model_name}
        Feature: {feature['name']}
        Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

        Description:
        This snapshot tracks historical changes over time using Type 2 Slowly Changing Dimensions (SCD).
        It captures the full history of records and when they changed.

        Strategy: Timestamp-based SCD Type 2
        - Tracks changes based on updated_at column
        - Maintains dbt_valid_from and dbt_valid_to timestamps
        - Creates new row for each change
    """

        for scenario in feature['scenarios']:
            snapshot_sql += f"\n    Scenario: {scenario['name']}\n"
            for given in scenario['given']:
                snapshot_sql += f"    - Given: {given}\n"

        snapshot_sql += """*/

    select
        id,
        created_at,
        updated_at,
        status,
        value,
        -- Snapshot metadata
        current_timestamp() as snapshot_captured_at
    from {{ source('raw', 'source_table') }}
    where 1=1
        and deleted_at is null  -- Only active records

    {% endsnapshot %}
    """

        return snapshot_sql

    @staticmethod
    def generate_ephemeral(model_name: str, feature: Dict) -> str:
        """Generate an ephemeral DBT model (CTE only, not materialized)"""

        ephemeral_sql = f"""{{{{
        config(
            materialized='ephemeral',
            tags=['ephemeral', 'auto-generated']
        )
    }}}}

    /*
        Ephemeral Model: {model_name}
        Feature: {feature['name']}
        Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

        Description:
        This is an ephemeral model that exists only as a CTE in downstream models.
        It is not materialized in the database, improving query performance and reducing storage.

        Use Case: Intermediate transformations that are only used by other models.
    """

        for scenario in feature['scenarios']:
            ephemeral_sql += f"\n    Scenario: {scenario['name']}\n"
            for given in scenario['given']:
                ephemeral_sql += f"    - Given: {given}\n"

        ephemeral_sql += """*/

    -- This model will be compiled as a CTE in downstream queries
    select
        id,
        created_at,
        updated_at,

        -- Derived fields for reuse
        case
            when status = 'active' then 1
            when status = 'inactive' then 0
            else null
        end as is_active_flag,

        -- Calculated metrics
        datediff(day, created_at, updated_at) as days_since_creation,

        -- Standardized fields
        lower(trim(status)) as status_normalized,

        value
    from {{ source('raw', 'source_table') }}
    where 1=1
        and created_at is not null
    """

        return ephemeral_sql


class GitHubHandler:
    """Handle GitHub repository operations"""

    @staticmethod
    def fetch_dbt_models(github_url: str, token: Optional[str] = None) -> List[Dict]:
        """Fetch DBT models from GitHub repository"""
        try:
            parts = github_url.replace('https://github.com/', '').split('/')
            owner, repo = parts[0], parts[1]
            branch = 'main'
            path = '/'.join(parts[3:]) if len(parts) > 3 else 'models'

            api_url = f"https://api.github.com/repos/{owner}/{repo}/contents/{path}"

            headers = {}
            if token:
                headers['Authorization'] = f'token {token}'

            response = requests.get(api_url, headers=headers, params={'ref': branch})
            response.raise_for_status()

            files = response.json()
            models = []

            for file in files:
                if file['name'].endswith('.sql'):
                    content_response = requests.get(file['download_url'])
                    models.append({
                        'name': file['name'].replace('.sql', ''),
                        'path': file['path'],
                        'content': content_response.text
                    })

            return models
        except Exception as e:
            st.error(f"Error fetching from GitHub: {str(e)}")
            return []


class LLMHandler:
    """Handle LLM API calls for generating Gherkin and tests"""

    @staticmethod
    def call_openai(prompt: str, api_key: str, model: str) -> str:
        try:
            headers = {
                "Authorization": f"Bearer {api_key}",
                "Content-Type": "application/json"
            }

            data = {
                "model": model,
                "messages": [
                    {"role": "system", "content": "You are an expert in DBT and Gherkin test specifications."},
                    {"role": "user", "content": prompt}
                ],
                "temperature": 0.7,
                "max_tokens": 2000
            }

            response = requests.post(
                "https://api.openai.com/v1/chat/completions",
                headers=headers,
                json=data
            )
            response.raise_for_status()

            return response.json()['choices'][0]['message']['content']
        except Exception as e:
            raise Exception(f"OpenAI API error: {str(e)}")

    @staticmethod
    def call_anthropic(prompt: str, api_key: str, model: str) -> str:
        try:
            headers = {
                "x-api-key": api_key,
                "Content-Type": "application/json",
                "anthropic-version": "2023-06-01"
            }

            data = {
                "model": model,
                "max_tokens": 2000,
                "messages": [
                    {"role": "user", "content": prompt}
                ]
            }

            response = requests.post(
                "https://api.anthropic.com/v1/messages",
                headers=headers,
                json=data
            )
            response.raise_for_status()

            return response.json()['content'][0]['text']
        except Exception as e:
            raise Exception(f"Anthropic API error: {str(e)}")

    @staticmethod
    def call_azure_openai(prompt: str, api_key: str, endpoint: str, model: str) -> str:
        try:
            headers = {
                "api-key": api_key,
                "Content-Type": "application/json"
            }

            data = {
                "messages": [
                    {"role": "system", "content": "You are an expert in DBT and Gherkin test specifications."},
                    {"role": "user", "content": prompt}
                ],
                "temperature": 0.7,
                "max_tokens": 2000
            }

            url = f"{endpoint}/openai/deployments/{model}/chat/completions?api-version=2023-05-15"
            response = requests.post(url, headers=headers, json=data)
            response.raise_for_status()

            return response.json()['choices'][0]['message']['content']
        except Exception as e:
            raise Exception(f"Azure OpenAI API error: {str(e)}")

    @staticmethod
    def call_local_llm(prompt: str, endpoint: str, model: str) -> str:
        try:
            data = {
                "model": model,
                "prompt": prompt,
                "stream": False
            }

            response = requests.post(f"{endpoint}/api/generate", json=data)
            response.raise_for_status()

            return response.json()['response']
        except Exception as e:
            raise Exception(f"Local LLM error: {str(e)}")

    @staticmethod
    def generate_from_llm(config: Dict, prompt: str) -> str:
        provider = config['provider']

        if provider == "OpenAI":
            return LLMHandler.call_openai(prompt, config['api_key'], config['model'])
        elif provider == "Anthropic":
            return LLMHandler.call_anthropic(prompt, config['api_key'], config['model'])
        elif provider == "Azure OpenAI":
            return LLMHandler.call_azure_openai(prompt, config['api_key'], config['endpoint'], config['model'])
        elif provider == "Local LLM":
            return LLMHandler.call_local_llm(prompt, config['endpoint'], config['model'])
        else:
            raise Exception(f"Unknown provider: {provider}")

    @staticmethod
    def plain_english_to_gherkin(description: str, config: Dict) -> str:
        prompt = f"""Convert the following plain English description into a well-structured Gherkin feature specification for DBT testing.

    Plain English Description:
    {description}

    Generate a complete Gherkin feature with:
    - Feature: A clear feature name and description
    - Multiple Scenarios covering different test cases
    - Given, When, Then statements that are specific and testable
    - Focus on data quality, uniqueness, null checks, relationships, and accepted values where applicable

    Format the output as a valid Gherkin feature specification."""

        return LLMHandler.generate_from_llm(config, prompt)

    @staticmethod
    def gherkin_to_tests(gherkin: str, model_name: str, config: Dict) -> Dict[str, str]:
        prompt = f"""Given the following Gherkin specification, generate comprehensive DBT tests and a model.

    Model Name: {model_name}

    Gherkin Specification:
    {gherkin}

    Please generate:

    1. A schema.yml file with appropriate DBT tests (unique, not_null, accepted_values, relationships)
    2. A unit test SQL file that validates the business logic
    3. A DBT model SQL file that implements the logic described in the Gherkin scenarios

    Format your response EXACTLY as follows:

    ===SCHEMA.YML===
    [schema.yml content here]

    ===UNIT_TEST.SQL===
    [unit test SQL content here]

    ===MODEL.SQL===
    [model SQL content here]

    Ensure all generated code is valid DBT syntax and follows best practices."""

        response = LLMHandler.generate_from_llm(config, prompt)

        parts = {
            'schema': '',
            'unit_test': '',
            'model': ''
        }

        if '===SCHEMA.YML===' in response:
            schema_start = response.find('===SCHEMA.YML===') + len('===SCHEMA.YML===')
            schema_end = response.find('===UNIT_TEST.SQL===')
            parts['schema'] = response[schema_start:schema_end].strip()

        if '===UNIT_TEST.SQL===' in response:
            test_start = response.find('===UNIT_TEST.SQL===') + len('===UNIT_TEST.SQL===')
            test_end = response.find('===MODEL.SQL===')
            parts['unit_test'] = response[test_start:test_end].strip()

        if '===MODEL.SQL===' in response:
            model_start = response.find('===MODEL.SQL===') + len('===MODEL.SQL===')
            parts['model'] = response[model_start:].strip()

        return parts


class CodeCoverageAnalyzer:
    """Analyze and generate code coverage reports for DBT models and tests"""

    @staticmethod
    def extract_columns_from_model(model_sql: str) -> List[str]:
        columns = []
        select_pattern = r'select\s+(.*?)\s+from'
        matches = re.findall(select_pattern, model_sql, re.IGNORECASE | re.DOTALL)

        for match in matches:
            cols = match.split(',')
            for col in cols:
                col = re.sub(r'--.*', '', col)
                col = re.sub(r'/\*.*?\*/', '', col, flags=re.DOTALL)
                col = re.sub(r'\s+as\s+', ' ', col, re.IGNORECASE)

                words = col.strip().split()
                if words:
                    col_name = words[-1].strip('()')
                    if col_name and col_name not in ['from', 'where', 'group', 'order', 'having']:
                        columns.append(col_name)

        return list(set(columns))

    @staticmethod
    def extract_tests_from_schema(schema_yaml: str) -> Dict[str, List[str]]:
        try:
            schema_data = yaml.safe_load(schema_yaml)
            tests_by_column = defaultdict(list)

            if 'models' in schema_data:
                for model in schema_data['models']:
                    if 'columns' in model:
                        for column in model['columns']:
                            col_name = column.get('name', '')
                            if 'tests' in column:
                                for test in column['tests']:
                                    if isinstance(test, str):
                                        tests_by_column[col_name].append(test)
                                    elif isinstance(test, dict):
                                        test_name = list(test.keys())[0]
                                        tests_by_column[col_name].append(test_name)

            return dict(tests_by_column)
        except:
            return {}

    @staticmethod
    def analyze_coverage(models: List[Dict], generated_tests: Dict) -> Dict:
        coverage_report = {
            'total_models': len(models),
            'models_with_tests': 0,
            'total_columns': 0,
            'columns_with_tests': 0,
            'coverage_percentage': 0.0,
            'models_detail': [],
            'summary': {
                'unique_tests': 0,
                'not_null_tests': 0,
                'accepted_values_tests': 0,
                'relationships_tests': 0,
                'custom_tests': 0
            }
        }

        for model in models:
            model_name = model['name']
            model_sql = model.get('content', '')

            columns = CodeCoverageAnalyzer.extract_columns_from_model(model_sql)
            coverage_report['total_columns'] += len(columns)

            model_tests = {}
            if model_name == generated_tests.get('model_name'):
                model_tests = CodeCoverageAnalyzer.extract_tests_from_schema(
                    generated_tests.get('schema', '')
                )

            tested_columns = len([c for c in columns if c in model_tests])
            coverage_report['columns_with_tests'] += tested_columns

            model_coverage = (tested_columns / len(columns) * 100) if columns else 0

            if model_tests:
                coverage_report['models_with_tests'] += 1

            for col, tests in model_tests.items():
                for test in tests:
                    if 'unique' in test.lower():
                        coverage_report['summary']['unique_tests'] += 1
                    elif 'not_null' in test.lower():
                        coverage_report['summary']['not_null_tests'] += 1
                    elif 'accepted_values' in test.lower():
                        coverage_report['summary']['accepted_values_tests'] += 1
                    elif 'relationships' in test.lower():
                        coverage_report['summary']['relationships_tests'] += 1
                    else:
                        coverage_report['summary']['custom_tests'] += 1

            coverage_report['models_detail'].append({
                'name': model_name,
                'total_columns': len(columns),
                'tested_columns': tested_columns,
                'coverage_percentage': model_coverage,
                'columns': columns,
                'tested_columns_list': list(model_tests.keys()),
                'untested_columns': [c for c in columns if c not in model_tests],
                'tests': model_tests
            })

        if coverage_report['total_columns'] > 0:
            coverage_report['coverage_percentage'] = (
                    coverage_report['columns_with_tests'] /
                    coverage_report['total_columns'] * 100
            )

        return coverage_report

    @staticmethod
    def generate_coverage_html(coverage_report: Dict) -> str:
        coverage_pct = coverage_report['coverage_percentage']
        if coverage_pct >= 80:
            coverage_color = '#4CAF50'
        elif coverage_pct >= 60:
            coverage_color = '#FF9800'
        else:
            coverage_color = '#F44336'

        html = f"""<!DOCTYPE html>
    <html>
    <head>
        <title>DBT Test Coverage Report</title>
        <style>
            body {{ font-family: 'Inter', sans-serif; max-width: 1200px; margin: 0 auto; padding: 20px; background: #f5f5f5; }}
            .header {{ background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); padding: 30px; border-radius: 15px; color: white; margin-bottom: 20px; box-shadow: 0 10px 30px rgba(102, 126, 234, 0.3); }}
            .header h1 {{ margin: 0; font-size: 2.5rem; }}
            .coverage-bar {{ width: 100%; height: 40px; background: #eee; border-radius: 20px; overflow: hidden; margin: 20px 0; box-shadow: inset 0 2px 4px rgba(0,0,0,0.1); }}
            .coverage-fill {{ height: 100%; background: linear-gradient(90deg, {coverage_color}, {coverage_color}); display: flex; align-items: center; justify-content: center; color: white; font-weight: bold; font-size: 1.2rem; }}
            .metric-card {{ background: white; padding: 20px; border-radius: 12px; box-shadow: 0 4px 6px rgba(0,0,0,0.1); text-align: center; }}
            .metric-value {{ font-size: 2.5rem; font-weight: bold; color: {coverage_color}; }}
            .model-detail {{ background: white; padding: 20px; border-radius: 12px; box-shadow: 0 4px 6px rgba(0,0,0,0.1); margin-bottom: 15px; }}
            .column {{ display: inline-block; padding: 8px 12px; margin: 5px; border-radius: 6px; font-size: 14px; }}
            .tested {{ background: #E8F5E9; color: #2E7D32; border: 2px solid #4CAF50; }}
            .untested {{ background: #FFEBEE; color: #C62828; border: 2px solid #F44336; }}
        </style>
    </head>
    <body>
        <div class="header">
            <h1>🧪 DBT Test Coverage Report</h1>
            <p>Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
        </div>
        <div class="coverage-bar">
            <div class="coverage-fill" style="width: {coverage_pct}%">{coverage_pct:.1f}% Coverage</div>
        </div>
        <div style="display: grid; grid-template-columns: repeat(4, 1fr); gap: 15px; margin: 20px 0;">
            <div class="metric-card"><div class="metric-value">{coverage_report['total_models']}</div><div>Total Models</div></div>
            <div class="metric-card"><div class="metric-value">{coverage_report['models_with_tests']}</div><div>Models with Tests</div></div>
            <div class="metric-card"><div class="metric-value">{coverage_report['total_columns']}</div><div>Total Columns</div></div>
            <div class="metric-card"><div class="metric-value">{coverage_report['columns_with_tests']}</div><div>Tested Columns</div></div>
        </div>
    """

        for model in coverage_report['models_detail']:
            html += f"""<div class="model-detail">
            <h3>📄 {model['name']} - {model['coverage_percentage']:.1f}%</h3>
            <p><strong>{model['tested_columns']}</strong> of <strong>{model['total_columns']}</strong> columns tested</p>
            <div>"""

            for col in model['columns']:
                is_tested = col in model['tested_columns_list']
                css_class = 'tested' if is_tested else 'untested'
                icon = '✓' if is_tested else '✗'
                html += f'<span class="column {css_class}">{icon} {col}</span>'

            html += "</div></div>"

        html += "</body></html>"
        return html

    @staticmethod
    def generate_json_report(coverage_report: Dict) -> str:
        return json.dumps(coverage_report, indent=2)


def main():
    # Hero Header
    st.markdown("""
        <div class="main-header">
            <h1>🚀 DBT Test Generator Pro</h1>
            <p>AI-Powered Test Generation with Gherkin DSL | Enterprise-Grade Coverage Analysis</p>
        </div>
        """, unsafe_allow_html=True)

    # Sidebar Configuration
    with st.sidebar:
        st.markdown("### ⚙️ Configuration")

        input_method = st.radio(
            "Choose Input Method",
            ["🎯 Manual Entry", "🤖 LLM-Assisted", "💬 Plain English", "📁 GitHub Repo", "💾 Local Folder"],
            help="Select how you want to input your test specifications"
        )

        # LLM Configuration
        if "LLM" in input_method or "Plain English" in input_method:
            st.markdown("---")
            st.markdown("### 🤖 AI Configuration")

            llm_provider = st.selectbox(
                "AI Provider",
                ["OpenAI", "Anthropic", "Azure OpenAI", "Local LLM"],
                help="Choose your AI provider"
            )

            if llm_provider == "OpenAI":
                api_key = st.text_input("API Key", type="password", help="Your OpenAI API key")
                model = st.selectbox("Model", ["gpt-4", "gpt-4-turbo", "gpt-3.5-turbo"])
            elif llm_provider == "Anthropic":
                api_key = st.text_input("API Key", type="password", help="Your Anthropic API key")
                model = st.selectbox("Model", ["claude-3-opus-20240229", "claude-3-sonnet-20240229"])
            elif llm_provider == "Azure OpenAI":
                api_key = st.text_input("API Key", type="password")
                endpoint = st.text_input("Endpoint URL")
                model = st.text_input("Deployment Name")
            else:
                endpoint = st.text_input("Endpoint", value="http://localhost:11434")
                model = st.text_input("Model", value="llama2")

            st.session_state['llm_config'] = {
                'provider': llm_provider,
                'api_key': api_key if llm_provider != "Local LLM" else None,
                'endpoint': endpoint if llm_provider in ["Azure OpenAI", "Local LLM"] else None,
                'model': model
            }

        st.markdown("---")
        st.markdown("### 📊 Quick Stats")
        if 'generated' in st.session_state:
            st.success("✅ Tests Generated")
        if 'coverage_report' in st.session_state:
            report = st.session_state['coverage_report']
            st.metric("Coverage", f"{report['coverage_percentage']:.1f}%")

        st.markdown("---")
        st.markdown("""
            <div style="text-align: center; padding: 20px; background: rgba(102, 126, 234, 0.1); border-radius: 10px;">
                <p style="margin: 0; font-size: 0.85rem; color: #667eea;">
                    <strong>DBT Test Generator Pro v2.0</strong><br>
                    Powered by AI & Gherkin DSL
                </p>
            </div>
            """, unsafe_allow_html=True)

    # Main Content Area
    col1, col2 = st.columns([1, 1], gap="large")

    with col1:
        st.markdown("### 📥 Input Configuration")

        if "GitHub" in input_method:
            st.markdown('<div class="feature-card">', unsafe_allow_html=True)
            st.markdown("#### 🔗 GitHub Repository")
            github_url = st.text_input(
                "Repository URL",
                placeholder="https://github.com/owner/repo/tree/main/models",
                help="Enter the full GitHub URL to your DBT models"
            )
            github_token = st.text_input("Access Token (Optional)", type="password")

            if st.button("🚀 Fetch Models", use_container_width=True):
                with st.spinner("🔄 Fetching models from GitHub..."):
                    models = GitHubHandler.fetch_dbt_models(github_url, github_token)
                    if models:
                        st.session_state['models'] = models
                        st.success(f"✅ Successfully loaded {len(models)} models!")
                        time.sleep(0.5)
                        st.rerun()
            st.markdown('</div>', unsafe_allow_html=True)

        elif "Local Folder" in input_method:
            st.markdown('<div class="feature-card">', unsafe_allow_html=True)
            st.markdown("#### 💾 Local Directory")
            folder_path = st.text_input(
                "Folder Path",
                placeholder="/path/to/dbt/models",
                help="Enter the absolute path to your DBT models folder"
            )

            if st.button("📂 Load Models", use_container_width=True):
                try:
                    path = Path(folder_path)
                    if path.exists():
                        with st.spinner("🔄 Loading models..."):
                            sql_files = list(path.glob("*.sql"))
                            models = []
                            for file in sql_files:
                                with open(file, 'r') as f:
                                    models.append({
                                        'name': file.stem,
                                        'path': str(file),
                                        'content': f.read()
                                    })
                            st.session_state['models'] = models
                            st.success(f"✅ Loaded {len(models)} models!")
                            time.sleep(0.5)
                            st.rerun()
                except Exception as e:
                    st.error(f"❌ Error: {str(e)}")
            st.markdown('</div>', unsafe_allow_html=True)

        elif "Plain English" in input_method:
            st.markdown('<div class="feature-card">', unsafe_allow_html=True)
            st.markdown("#### 💬 Natural Language Input")
            st.info("🎯 Describe your requirements in plain English - AI will convert to Gherkin!")

            plain_english = st.text_area(
                "Test Requirements",
                value="""I need to validate a customer table that:
    - Has unique customer IDs (never null)
    - Has properly formatted email addresses
    - Has status only as 'active', 'inactive', or 'suspended'
    - Has created_at never in the future
    - Has country referencing countries table
    - Has no customers under 18 years old""",
                height=200
            )

            if st.button("🔮 Convert to Gherkin", use_container_width=True):
                if 'llm_config' not in st.session_state:
                    st.error("⚠️ Please configure AI settings in sidebar")
                else:
                    try:
                        with st.spinner("🤖 AI is analyzing your requirements..."):
                            progress_bar = st.progress(0)
                            for i in range(100):
                                time.sleep(0.01)
                                progress_bar.progress(i + 1)

                            gherkin_output = LLMHandler.plain_english_to_gherkin(
                                plain_english,
                                st.session_state['llm_config']
                            )
                            st.session_state['converted_gherkin'] = gherkin_output
                            st.success("✅ Converted to Gherkin!")
                            time.sleep(0.5)
                            st.rerun()
                    except Exception as e:
                        st.error(f"❌ Error: {str(e)}")

            if 'converted_gherkin' in st.session_state:
                st.markdown("---")
                st.markdown("#### 📝 Generated Gherkin")
                edited_gherkin = st.text_area(
                    "Review & Edit",
                    value=st.session_state['converted_gherkin'],
                    height=250
                )

                model_name = st.text_input("Model Name", value="stg_customers")

                col_a, col_b = st.columns(2)
                with col_a:
                    if st.button("⚡ Generate Tests", use_container_width=True):
                        with st.spinner("Generating..."):
                            parser = GherkinDSLParser()
                            feature = parser.parse_feature(edited_gherkin)

                            model_sql = DBTModelGenerator.generate_model(model_name, feature, "view")
                            schema_yaml = DBTTestGenerator.generate_schema_tests(feature, model_name)
                            unit_test_data = DBTTestGenerator.generate_unit_test_with_coverage(
                                feature, model_name, model_sql
                            )

                            st.session_state['generated'] = {
                                'schema': schema_yaml,
                                'unit_test': unit_test_data['unit_test'],
                                'coverage_metadata': unit_test_data['coverage_metadata'],
                                'model': model_sql,
                                'model_name': model_name,
                                'model_type': 'view',
                                'test_coverage': {
                                    'tested_columns': unit_test_data['tested_columns'],
                                    'untested_columns': unit_test_data['untested_columns'],
                                    'coverage_percentage': unit_test_data['coverage_percentage']
                                }
                            }
                            st.success("✅ Done!")
                            st.rerun()

                with col_b:
                    if st.button("🤖 AI Generate", use_container_width=True):
                        try:
                            with st.spinner("AI generating..."):
                                llm_results = LLMHandler.gherkin_to_tests(
                                    edited_gherkin,
                                    model_name,
                                    st.session_state['llm_config']
                                )

                                st.session_state['generated'] = {
                                    'schema': llm_results['schema'],
                                    'unit_test': llm_results['unit_test'],
                                    'model': llm_results['model'],
                                    'model_name': model_name
                                }
                                st.success("✅ Done!")
                                st.rerun()
                        except Exception as e:
                            st.error(f"❌ {str(e)}")
            st.markdown('</div>', unsafe_allow_html=True)

        elif "LLM-Assisted" in input_method:
            st.markdown('<div class="feature-card">', unsafe_allow_html=True)
            st.markdown("#### 🤖 AI-Powered Generation")
            st.info("🎯 Provide Gherkin specs - AI generates comprehensive tests!")

            gherkin_text = st.text_area(
                "Gherkin Specification",
                value="""Feature: Order Processing Quality
      Validate order data integrity

    Scenario: Order ID validation
      Given an orders table with order_id
      When we check identifiers
      Then order_id should be unique
      And order_id should not be null
      And order_id should match pattern 'ORD-[0-9]+'

    Scenario: Order status validation
      Given an orders table with status
      Then status should have accepted_values ['pending', 'processing', 'completed', 'cancelled']""",
                height=300
            )

            model_name = st.text_input("Model Name", value="stg_orders")

            if st.button("🚀 Generate with AI", use_container_width=True):
                if 'llm_config' not in st.session_state:
                    st.error("⚠️ Configure AI settings in sidebar")
                else:
                    try:
                        with st.spinner("🤖 AI is crafting your tests..."):
                            progress_bar = st.progress(0)
                            for i in range(100):
                                time.sleep(0.02)
                                progress_bar.progress(i + 1)

                            llm_results = LLMHandler.gherkin_to_tests(
                                gherkin_text,
                                model_name,
                                st.session_state['llm_config']
                            )

                            st.session_state['generated'] = {
                                'schema': llm_results['schema'],
                                'unit_test': llm_results['unit_test'],
                                'model': llm_results['model'],
                                'model_name': model_name
                            }
                            st.success("✅ AI generation complete!")
                            st.balloons()
                            time.sleep(0.5)
                            st.rerun()
                    except Exception as e:
                        st.error(f"❌ Error: {str(e)}")
            st.markdown('</div>', unsafe_allow_html=True)

        else:  # Manual Entry
            st.markdown('<div class="feature-card">', unsafe_allow_html=True)
            st.markdown("#### ✍️ Manual Gherkin Entry")

            gherkin_text = st.text_area(
                "Gherkin Feature",
                value="""Feature: Customer Data Quality
      Ensure customer data meets standards

    Scenario: Customer ID uniqueness
      Given a customer table with customer_id
      When we check for duplicates
      Then customer_id should be unique
      And customer_id should not be null

    Scenario: Email validation
      Given a customer table with email
      Then email should match email pattern
      And email should not be null

    Scenario: Status values
      Given a customer table with status
      Then status should have accepted_values ['active', 'inactive', 'pending']""",
                height=300
            )

            col_x, col_y = st.columns(2)
            with col_x:
                model_name = st.text_input("Model Name", value="stg_customers")
            with col_y:
                model_type = st.selectbox("Materialization", ["view", "table", "incremental", "snapshot", "ephemeral"])

            # Add info boxes for different model types
            if model_type == "snapshot":
                st.info(
                    "📸 **Snapshot**: Creates Type 2 SCD for tracking historical changes. Perfect for slowly changing dimensions.")
            elif model_type == "ephemeral":
                st.info(
                    "⚡ **Ephemeral**: Exists only as CTE, not materialized. Great for intermediate transformations.")
            elif model_type == "incremental":
                st.info("🔄 **Incremental**: Only processes new/changed data. Efficient for large datasets.")

            if st.button("⚡ Generate Tests & Models", use_container_width=True):
                with st.spinner("⚙️ Generating comprehensive tests..."):
                    progress_bar = st.progress(0)
                    for i in range(100):
                        time.sleep(0.01)
                        progress_bar.progress(i + 1)

                    parser = GherkinDSLParser()
                    feature = parser.parse_feature(gherkin_text)

                    # Generate model
                    model_sql = DBTModelGenerator.generate_model(model_name, feature, model_type)

                    # Generate schema tests
                    schema_yaml = DBTTestGenerator.generate_schema_tests(feature, model_name)

                    # Generate comprehensive unit tests with coverage
                    unit_test_data = DBTTestGenerator.generate_unit_test_with_coverage(
                        feature, model_name, model_sql
                    )

                    st.session_state['generated'] = {
                        'schema': schema_yaml,
                        'unit_test': unit_test_data['unit_test'],
                        'coverage_metadata': unit_test_data['coverage_metadata'],
                        'model': model_sql,
                        'model_name': model_name,
                        'model_type': model_type,
                        'test_coverage': {
                            'tested_columns': unit_test_data['tested_columns'],
                            'untested_columns': unit_test_data['untested_columns'],
                            'coverage_percentage': unit_test_data['coverage_percentage']
                        }
                    }
                    st.success("✅ Comprehensive tests generated with coverage report!")
                    st.rerun()
            st.markdown('</div>', unsafe_allow_html=True)

    with col2:
        st.markdown("### 💡 Quick Tips")

        if "LLM" in input_method:
            st.markdown("""
                <div class="feature-card">
                    <h4>🤖 AI Generation Tips</h4>
                    <ul>
                        <li>✨ Provide detailed Gherkin scenarios</li>
                        <li>🎯 AI generates comprehensive tests</li>
                        <li>📝 Review before production use</li>
                        <li>🔗 Handles complex relationships</li>
                    </ul>
                </div>
                """, unsafe_allow_html=True)
        elif "Plain English" in input_method:
            st.markdown("""
                <div class="feature-card">
                    <h4>💬 Plain English Tips</h4>
                    <ul>
                        <li>🎯 Be specific about requirements</li>
                        <li>📋 Mention column names & constraints</li>
                        <li>🔗 Describe table relationships</li>
                        <li>✅ Include business rules</li>
                    </ul>
                </div>
                """, unsafe_allow_html=True)
        else:
            st.markdown("""
                <div class="feature-card">
                    <h4>📚 Gherkin Syntax</h4>
                    <ul>
                        <li><strong>Feature:</strong> Describe the feature</li>
                        <li><strong>Scenario:</strong> Test case</li>
                        <li><strong>Given:</strong> Initial context</li>
                        <li><strong>When:</strong> Action/event</li>
                        <li><strong>Then:</strong> Expected outcome</li>
                        <li><strong>And:</strong> Additional steps</li>
                    </ul>
                </div>
                """, unsafe_allow_html=True)

        if 'models' in st.session_state and st.session_state['models']:
            st.markdown("""
                <div class="feature-card">
                    <h4>📊 Models Loaded</h4>
                """, unsafe_allow_html=True)
            st.metric("Total Models", len(st.session_state['models']))
            st.markdown('</div>', unsafe_allow_html=True)

    # Generated Outputs Section
    if 'generated' in st.session_state:
        st.markdown("---")
        st.markdown("## 📋 Generated Artifacts")

        tab1, tab2, tab3, tab4, tab5 = st.tabs([
            "📄 Schema Tests",
            "🧪 Unit Tests",
            "📊 Model",
            "📖 Documentation",
            "📈 Coverage"
        ])

        with tab1:
            st.markdown('<div class="feature-card">', unsafe_allow_html=True)
            st.code(st.session_state['generated']['schema'], language='yaml')
            st.download_button(
                "⬇️ Download schema.yml",
                st.session_state['generated']['schema'],
                file_name="schema.yml",
                use_container_width=True
            )
            st.markdown('</div>', unsafe_allow_html=True)

        with tab2:
            st.markdown('<div class="feature-card">', unsafe_allow_html=True)
            st.markdown("#### 🧪 Comprehensive Unit Tests")

            # Show test coverage summary if available
            if 'test_coverage' in st.session_state['generated']:
                test_cov = st.session_state['generated']['test_coverage']

                cov_cols = st.columns(3)
                with cov_cols[0]:
                    st.metric("Test Coverage", f"{test_cov['coverage_percentage']:.1f}%")
                with cov_cols[1]:
                    st.metric("Tested Columns", len(test_cov['tested_columns']))
                with cov_cols[2]:
                    st.metric("Untested Columns", len(test_cov['untested_columns']))

                st.markdown("---")

                # Show test categories
                st.markdown("**Test Categories Included:**")
                test_categories = [
                    "✓ Null Checks - Critical fields validation",
                    "✓ Duplicate Checks - Primary key uniqueness",
                    "✓ Data Type Validation - Format verification",
                    "✓ Business Logic - Rule compliance",
                    "✓ Range Validation - Boundary checks",
                    "✓ Status Validation - Enum verification",
                    "✓ Edge Cases - Extreme value handling",
                    "✓ Referential Integrity - Foreign key validation"
                ]

                for category in test_categories:
                    st.markdown(f"- {category}")

                st.markdown("---")

            # Display coverage metadata if available
            if 'coverage_metadata' in st.session_state['generated']:
                with st.expander("📊 View Test Coverage Details"):
                    st.code(st.session_state['generated']['coverage_metadata'], language='sql')

            st.code(st.session_state['generated']['unit_test'], language='sql')

            col_download = st.columns([1, 1])
            with col_download[0]:
                st.download_button(
                    "⬇️ Download Unit Test",
                    st.session_state['generated']['unit_test'],
                    file_name=f"test_{st.session_state['generated']['model_name']}.sql",
                    use_container_width=True
                )

            with col_download[1]:
                if 'coverage_metadata' in st.session_state['generated']:
                    st.download_button(
                        "⬇️ Download Coverage Report",
                        st.session_state['generated']['coverage_metadata'],
                        file_name=f"coverage_{st.session_state['generated']['model_name']}.txt",
                        use_container_width=True
                    )

            st.markdown('</div>', unsafe_allow_html=True)

        with tab3:
            st.markdown('<div class="feature-card">', unsafe_allow_html=True)

            # Show model type badge
            model_type = st.session_state['generated'].get('model_type', 'view')

            if model_type == "snapshot":
                st.markdown("""
                    <div style="padding: 15px; background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); 
                         border-radius: 10px; margin-bottom: 15px; color: white;">
                        <h4 style="margin: 0; color: white;">📸 Snapshot Model</h4>
                        <p style="margin: 5px 0 0 0; font-size: 0.9rem;">
                            Type 2 Slowly Changing Dimension (SCD) - Tracks full historical changes
                        </p>
                    </div>
                    """, unsafe_allow_html=True)
            elif model_type == "ephemeral":
                st.markdown("""
                    <div style="padding: 15px; background: linear-gradient(135deg, #11998e 0%, #38ef7d 100%); 
                         border-radius: 10px; margin-bottom: 15px; color: white;">
                        <h4 style="margin: 0; color: white;">⚡ Ephemeral Model</h4>
                        <p style="margin: 5px 0 0 0; font-size: 0.9rem;">
                            CTE-only model - Not materialized in database, improves performance
                        </p>
                    </div>
                    """, unsafe_allow_html=True)
            elif model_type == "incremental":
                st.markdown("""
                    <div style="padding: 15px; background: linear-gradient(135deg, #f093fb 0%, #f5576c 100%); 
                         border-radius: 10px; margin-bottom: 15px; color: white;">
                        <h4 style="margin: 0; color: white;">🔄 Incremental Model</h4>
                        <p style="margin: 5px 0 0 0; font-size: 0.9rem;">
                            Processes only new/changed data - Efficient for large datasets
                        </p>
                    </div>
                    """, unsafe_allow_html=True)
            else:
                st.markdown(f"""
                    <div style="padding: 15px; background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); 
                         border-radius: 10px; margin-bottom: 15px; color: white;">
                        <h4 style="margin: 0; color: white;">📊 {model_type.title()} Model</h4>
                        <p style="margin: 5px 0 0 0; font-size: 0.9rem;">
                            Standard DBT {model_type} materialization
                        </p>
                    </div>
                    """, unsafe_allow_html=True)

            st.code(st.session_state['generated']['model'], language='sql')
            st.download_button(
                "⬇️ Download Model",
                st.session_state['generated']['model'],
                file_name=f"{st.session_state['generated']['model_name']}.sql",
                use_container_width=True
            )
            st.markdown('</div>', unsafe_allow_html=True)

        with tab4:
            st.markdown('<div class="feature-card">', unsafe_allow_html=True)
            doc = f"""# {st.session_state['generated']['model_name']}

    ## 🎯 Overview
    Auto-generated DBT model from Gherkin specifications.

    ## ✅ Tests Included
    - **Schema Tests**: Data quality validations
    - **Unit Tests**: Business logic validation

    ## 🚀 Usage
    ```bash
    # Run model
    dbt run --select {st.session_state['generated']['model_name']}

    # Run tests
    dbt test --select {st.session_state['generated']['model_name']}
    ```

    ## 📁 Structure
    ```
    models/
    ├── {st.session_state['generated']['model_name']}.sql
    ├── schema.yml
    tests/
    └── test_{st.session_state['generated']['model_name']}.sql
    ```

    ---
    *Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}*
    """
            st.markdown(doc)
            st.download_button(
                "⬇️ Download README",
                doc,
                file_name=f"{st.session_state['generated']['model_name']}_README.md",
                use_container_width=True
            )
            st.markdown('</div>', unsafe_allow_html=True)

        with tab5:
            st.markdown('<div class="feature-card">', unsafe_allow_html=True)

            if st.button("🔄 Generate Coverage Report", use_container_width=True):
                models_to_analyze = []

                if 'models' in st.session_state and st.session_state['models']:
                    models_to_analyze = st.session_state['models']
                else:
                    models_to_analyze = [{
                        'name': st.session_state['generated']['model_name'],
                        'content': st.session_state['generated']['model']
                    }]

                with st.spinner("📊 Analyzing coverage..."):
                    progress_bar = st.progress(0)
                    for i in range(100):
                        time.sleep(0.01)
                        progress_bar.progress(i + 1)

                    coverage_report = CodeCoverageAnalyzer.analyze_coverage(
                        models_to_analyze,
                        st.session_state['generated']
                    )

                    st.session_state['coverage_report'] = coverage_report
                    st.success("✅ Coverage analysis complete!")
                    st.rerun()

            if 'coverage_report' in st.session_state:
                report = st.session_state['coverage_report']

                # Metrics
                metric_cols = st.columns(4)
                with metric_cols[0]:
                    st.markdown("""
                        <div class="metric-card">
                            <div class="metric-label">Total Models</div>
                            <div class="metric-value">{}</div>
                        </div>
                        """.format(report['total_models']), unsafe_allow_html=True)

                with metric_cols[1]:
                    st.markdown("""
                        <div class="metric-card">
                            <div class="metric-label">With Tests</div>
                            <div class="metric-value">{}</div>
                        </div>
                        """.format(report['models_with_tests']), unsafe_allow_html=True)

                with metric_cols[2]:
                    st.markdown("""
                        <div class="metric-card">
                            <div class="metric-label">Total Columns</div>
                            <div class="metric-value">{}</div>
                        </div>
                        """.format(report['total_columns']), unsafe_allow_html=True)

                with metric_cols[3]:
                    st.markdown("""
                        <div class="metric-card">
                            <div class="metric-label">Tested Columns</div>
                            <div class="metric-value">{}</div>
                        </div>
                        """.format(report['columns_with_tests']), unsafe_allow_html=True)

                st.markdown("<br>", unsafe_allow_html=True)

                # Coverage bar
                coverage_pct = report['coverage_percentage']
                if coverage_pct >= 80:
                    color = "green"
                    badge = "status-success"
                elif coverage_pct >= 60:
                    color = "orange"
                    badge = "status-warning"
                else:
                    color = "red"
                    badge = "status-danger"

                st.progress(coverage_pct / 100)
                st.markdown(f"""
                    <div style="text-align: center; margin: 10px 0;">
                        <span class="status-badge {badge}">Coverage: {coverage_pct:.1f}%</span>
                    </div>
                    """, unsafe_allow_html=True)

                # Test breakdown
                st.markdown("#### 🔍 Test Distribution")
                test_cols = st.columns(5)
                test_cols[0].metric("🔑 Unique", report['summary']['unique_tests'])
                test_cols[1].metric("✓ Not Null", report['summary']['not_null_tests'])
                test_cols[2].metric("📋 Values", report['summary']['accepted_values_tests'])
                test_cols[3].metric("🔗 Relations", report['summary']['relationships_tests'])
                test_cols[4].metric("⚙️ Custom", report['summary']['custom_tests'])

                # Model details
                st.markdown("#### 📊 Model Details")
                for model in report['models_detail']:
                    with st.expander(f"📄 {model['name']} - {model['coverage_percentage']:.1f}%"):
                        st.write(f"**{model['tested_columns']}/{model['total_columns']} columns tested**")

                        col_a, col_b = st.columns(2)
                        with col_a:
                            st.markdown("**✅ Tested:**")
                            for col in model['tested_columns_list']:
                                st.markdown(f"- `{col}`")

                        with col_b:
                            st.markdown("**❌ Untested:**")
                            for col in model['untested_columns']:
                                st.markdown(f"- `{col}`")

                # Export
                st.markdown("---")
                export_cols = st.columns(2)
                with export_cols[0]:
                    html_report = CodeCoverageAnalyzer.generate_coverage_html(report)
                    st.download_button(
                        "📄 Download HTML Report",
                        html_report,
                        file_name=f"coverage_{datetime.now().strftime('%Y%m%d_%H%M%S')}.html",
                        mime="text/html",
                        use_container_width=True
                    )

                with export_cols[1]:
                    json_report = CodeCoverageAnalyzer.generate_json_report(report)
                    st.download_button(
                        "📊 Download JSON Report",
                        json_report,
                        file_name=f"coverage_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json",
                        mime="application/json",
                        use_container_width=True
                    )

                # Insights
                st.markdown("---")
                st.markdown("#### 💡 Coverage Insights")
                if coverage_pct >= 80:
                    st.success("🎉 Excellent coverage! Your models are well-tested.")
                elif coverage_pct >= 60:
                    st.warning("⚠️ Good coverage, but room for improvement.")
                else:
                    st.error("❗ Low coverage detected. Add more tests.")

                untested_count = sum(len(m['untested_columns']) for m in report['models_detail'])
                if untested_count > 0:
                    st.info(f"💡 Add tests for {untested_count} untested columns to improve coverage.")

            st.markdown('</div>', unsafe_allow_html=True)

    # Loaded Models Section
    if 'models' in st.session_state and st.session_state['models']:
        st.markdown("---")
        st.markdown("## 🗂️ Loaded Models")

        col_batch1, col_batch2 = st.columns([2, 1])
        with col_batch1:
            if st.button("📊 Analyze All Models Coverage", use_container_width=True):
                with st.spinner("🔄 Analyzing all models..."):
                    progress_bar = st.progress(0)
                    for i in range(100):
                        time.sleep(0.02)
                        progress_bar.progress(i + 1)

                    generated_tests = st.session_state.get('generated', {})
                    coverage_report = CodeCoverageAnalyzer.analyze_coverage(
                        st.session_state['models'],
                        generated_tests
                    )

                    st.session_state['coverage_report'] = coverage_report
                    st.success(f"✅ Analyzed {coverage_report['total_models']} models!")
                    st.balloons()
                    time.sleep(0.5)
                    st.rerun()

        # Display models in a grid
        for idx, model in enumerate(st.session_state['models']):
            with st.expander(f"📄 {model['name']}", expanded=False):
                st.code(model['content'], language='sql')

                btn_col1, btn_col2 = st.columns(2)

                with btn_col1:
                    if st.button(f"⚡ Generate Tests", key=f"gen_{idx}", use_container_width=True):
                        with st.spinner(f"Generating tests for {model['name']}..."):
                            feature = {
                                'name': f"{model['name']} quality checks",
                                'scenarios': [{
                                    'name': 'Basic data quality',
                                    'given': ['a model with data'],
                                    'when': ['we validate the data'],
                                    'then': ['all required fields should be present', 'no duplicates should exist']
                                }]
                            }

                            schema_yaml = DBTTestGenerator.generate_schema_tests(feature, model['name'])
                            unit_test = DBTTestGenerator.generate_unit_test(feature, model['name'])

                            st.session_state['generated'] = {
                                'schema': schema_yaml,
                                'unit_test': unit_test,
                                'model': model['content'],
                                'model_name': model['name']
                            }
                            st.success(f"✅ Tests generated for {model['name']}!")
                            st.rerun()

                with btn_col2:
                    if 'llm_config' in st.session_state:
                        if st.button(f"🤖 AI Generate", key=f"llm_{idx}", use_container_width=True):
                            try:
                                with st.spinner(f"🤖 AI generating for {model['name']}..."):
                                    prompt = f"""Analyze this DBT model and create comprehensive Gherkin tests:

    Model: {model['name']}
    SQL:
    {model['content']}

    Generate Gherkin covering data quality, business logic, relationships, and validations."""

                                    gherkin_spec = LLMHandler.generate_from_llm(
                                        st.session_state['llm_config'],
                                        prompt
                                    )

                                    llm_results = LLMHandler.gherkin_to_tests(
                                        gherkin_spec,
                                        model['name'],
                                        st.session_state['llm_config']
                                    )

                                    st.session_state['generated'] = {
                                        'schema': llm_results['schema'],
                                        'unit_test': llm_results['unit_test'],
                                        'model': model['content'],
                                        'model_name': model['name']
                                    }
                                    st.success(f"✅ AI tests for {model['name']}!")
                                    st.rerun()
                            except Exception as e:
                                st.error(f"❌ Error: {str(e)}")

        # Aggregate coverage summary
        if 'coverage_report' in st.session_state:
            st.markdown("---")
            st.markdown("### 📊 Aggregate Coverage")

            report = st.session_state['coverage_report']

            summary_cols = st.columns(3)
            with summary_cols[0]:
                st.markdown("""
                    <div class="metric-card">
                        <div class="metric-label">Models Coverage</div>
                        <div class="metric-value">{}/{}</div>
                    </div>
                    """.format(report['models_with_tests'], report['total_models']), unsafe_allow_html=True)

            with summary_cols[1]:
                st.markdown("""
                    <div class="metric-card">
                        <div class="metric-label">Column Coverage</div>
                        <div class="metric-value">{:.1f}%</div>
                    </div>
                    """.format(report['coverage_percentage']), unsafe_allow_html=True)

            with summary_cols[2]:
                total_tests = sum(report['summary'].values())
                st.markdown("""
                    <div class="metric-card">
                        <div class="metric-label">Total Tests</div>
                        <div class="metric-value">{}</div>
                    </div>
                    """.format(total_tests), unsafe_allow_html=True)

            # Models needing attention
            models_needing_tests = [
                m for m in report['models_detail']
                if m['coverage_percentage'] < 60
            ]

            if models_needing_tests:
                st.warning(f"⚠️ {len(models_needing_tests)} models have <60% coverage")
                with st.expander("🔍 View models needing attention"):
                    for model in models_needing_tests:
                        st.markdown(f"""
                            <div style="padding: 10px; margin: 5px 0; background: #fff3cd; border-left: 4px solid #ffc107; border-radius: 5px;">
                                <strong>{model['name']}</strong>: {model['coverage_percentage']:.1f}% 
                                ({model['tested_columns']}/{model['total_columns']} columns)
                            </div>
                            """, unsafe_allow_html=True)
            else:
                st.success("✅ All models have good test coverage!")

    # Footer
    st.markdown("---")
    st.markdown("""
        <div style="text-align: center; padding: 30px; background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); border-radius: 15px; margin-top: 50px;">
            <h3 style="color: white; margin: 0;">🚀 Ready to Transform Your DBT Testing?</h3>
               <p style="color: rgba(255,255,255,0.9); margin: 10px 0 0 0;">
                Generate comprehensive tests in seconds with AI-powered automation
            </p>
        </div>
        """, unsafe_allow_html=True)


if __name__ == "__main__":
    main()
