import streamlit as st
import re
from datetime import datetime

# Page configuration
st.set_page_config(page_title="DBT Test-Driven Generator", layout="wide", page_icon="🧪")

# Initialize session state
if 'gherkin_scenarios' not in st.session_state:
    st.session_state.gherkin_scenarios = []
if 'generated_tests' not in st.session_state:
    st.session_state.generated_tests = {}
if 'generated_models' not in st.session_state:
    st.session_state.generated_models = {}

# Helper Functions
def parse_gherkin(gherkin_text):
    """Parse Gherkin syntax into structured scenarios"""
    scenarios = []
    current_scenario = None
    
    lines = gherkin_text.strip().split('\n')
    
    for line in lines:
        line = line.strip()
        if not line or line.startswith('#'):
            continue
            
        if line.startswith('Feature:'):
            feature_name = line.replace('Feature:', '').strip()
            continue
            
        if line.startswith('Scenario:'):
            if current_scenario:
                scenarios.append(current_scenario)
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
            # Add to the last category
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
        scenarios.append(current_scenario)
    
    return scenarios

def generate_dbt_schema_test(scenario, model_name):
    """Generate DBT schema.yml test configuration"""
    test_name = scenario['name'].lower().replace(' ', '_')
    
    schema_yml = f"""# Test: {scenario['name']}
version: 2

models:
  - name: {model_name}
    description: "{scenario['name']}"
    columns:"""
    
    # Extract column tests from Then statements
    for then_stmt in scenario['then']:
        if 'not null' in then_stmt.lower():
            col_match = re.search(r'column[s]?\s+(\w+)', then_stmt, re.IGNORECASE)
            if col_match:
                col_name = col_match.group(1)
                schema_yml += f"""
      - name: {col_name}
        tests:
          - not_null"""
        
        if 'unique' in then_stmt.lower():
            col_match = re.search(r'column[s]?\s+(\w+)', then_stmt, re.IGNORECASE)
            if col_match:
                col_name = col_match.group(1)
                schema_yml += f"""
      - name: {col_name}
        tests:
          - unique"""
        
        if 'relationship' in then_stmt.lower() or 'references' in then_stmt.lower():
            col_match = re.search(r'column[s]?\s+(\w+)', then_stmt, re.IGNORECASE)
            ref_match = re.search(r'(?:to|from|in)\s+(\w+)', then_stmt, re.IGNORECASE)
            if col_match and ref_match:
                col_name = col_match.group(1)
                ref_table = ref_match.group(1)
                schema_yml += f"""
      - name: {col_name}
        tests:
          - relationships:
              to: ref('{ref_table}')
              field: id"""
        
        if 'accepted_values' in then_stmt.lower() or 'one of' in then_stmt.lower():
            col_match = re.search(r'column[s]?\s+(\w+)', then_stmt, re.IGNORECASE)
            values_match = re.findall(r"'([^']+)'", then_stmt)
            if col_match:
                col_name = col_match.group(1)
                values_str = ', '.join([f"'{v}'" for v in values_match])
                schema_yml += f"""
      - name: {col_name}
        tests:
          - accepted_values:
              values: [{values_str}]"""
    
    return schema_yml

def generate_dbt_singular_test(scenario, model_name):
    """Generate DBT singular test (SQL file)"""
    test_name = scenario['name'].lower().replace(' ', '_')
    
    sql_test = f"""-- Test: {scenario['name']}
-- Description: {' '.join(scenario['given'])}

"""
    
    # Build SELECT statement based on Then conditions
    conditions = []
    for then_stmt in scenario['then']:
        if 'should be' in then_stmt.lower() and 'rows' in then_stmt.lower():
            count_match = re.search(r'(\d+)\s+rows?', then_stmt)
            if count_match:
                expected_count = count_match.group(1)
                sql_test += f"""-- Expecting {expected_count} rows
select
    count(*) as actual_count
from {{{{ ref('{model_name}') }}}}
having count(*) != {expected_count}
"""
        elif 'greater than' in then_stmt.lower():
            col_match = re.search(r'column[s]?\s+(\w+)', then_stmt, re.IGNORECASE)
            val_match = re.search(r'greater than\s+(\d+)', then_stmt, re.IGNORECASE)
            if col_match and val_match:
                col_name = col_match.group(1)
                value = val_match.group(1)
                conditions.append(f"{col_name} <= {value}")
        
        elif 'no duplicates' in then_stmt.lower():
            col_match = re.search(r'column[s]?\s+(\w+)', then_stmt, re.IGNORECASE)
            if col_match:
                col_name = col_match.group(1)
                sql_test += f"""-- Checking for duplicates in {col_name}
select
    {col_name},
    count(*) as duplicate_count
from {{{{ ref('{model_name}') }}}}
group by {col_name}
having count(*) > 1
"""
    
    if conditions:
        sql_test += f"""-- Validation checks
select *
from {{{{ ref('{model_name}') }}}}
where {' or '.join(conditions)}
"""
    
    return sql_test

def generate_dbt_model(scenario, model_name, source_table):
    """Generate DBT model SQL"""
    
    model_sql = f"""{{{{
  config(
    materialized='table',
    tags=['test_driven', '{model_name}']
  )
}}}}

-- Model: {model_name}
-- Generated from scenario: {scenario['name']}
-- {' '.join(scenario['given'])}

with source_data as (
    select *
    from {{{{ source('raw', '{source_table}') }}}}
),

"""
    
    # Add transformation logic based on When statements
    transformations = []
    for when_stmt in scenario['when']:
        if 'filter' in when_stmt.lower() or 'where' in when_stmt.lower():
            transformations.append('filtered_data')
            condition_match = re.search(r'where\s+(.+)', when_stmt, re.IGNORECASE)
            if condition_match:
                condition = condition_match.group(1)
                model_sql += f"""filtered_data as (
    select *
    from source_data
    where {condition}
),

"""
        
        if 'aggregate' in when_stmt.lower() or 'group' in when_stmt.lower():
            transformations.append('aggregated_data')
            model_sql += """aggregated_data as (
    select
        -- Add your grouping columns here
        count(*) as record_count,
        sum(amount) as total_amount
    from """ + (transformations[-1] if transformations else 'source_data') + """
    group by 1
),

"""
        
        if 'join' in when_stmt.lower():
            table_match = re.search(r'join\s+(\w+)', when_stmt, re.IGNORECASE)
            if table_match:
                join_table = table_match.group(1)
                transformations.append('joined_data')
                model_sql += f"""joined_data as (
    select
        a.*,
        b.additional_field
    from """ + (transformations[-1] if transformations else 'source_data') + f""" a
    left join {{{{ ref('{join_table}') }}}} b
        on a.id = b.foreign_key
),

"""
    
    # Final select
    final_from = transformations[-1] if transformations else 'source_data'
    model_sql += f"""final as (
    select
        *,
        current_timestamp as dbt_loaded_at
    from {final_from}
)

select * from final
"""
    
    return model_sql

def generate_data_test(scenario, model_name):
    """Generate custom data test macro"""
    test_name = scenario['name'].lower().replace(' ', '_')
    
    macro_sql = f"""-- Custom data test: {test_name}
{{% macro test_{test_name}(model) %}}

select *
from {{{{ model }}}}
where
    -- Add your custom test logic here based on:
    -- Given: {' '.join(scenario['given'])}
    -- Then: {' '.join(scenario['then'])}
    1=1  -- Replace with actual test conditions

{{% endmacro %}}
"""
    
    return macro_sql

# Streamlit UI
st.title("🧪 DBT Test-Driven Development Generator")
st.markdown("Generate comprehensive DBT tests and models using Gherkin BDD syntax")

# Sidebar
with st.sidebar:
    st.header("⚙️ Configuration")
    model_name = st.text_input("Model Name", value="my_model", help="Name of the DBT model to generate")
    source_table = st.text_input("Source Table", value="raw_table", help="Source table name")
    
    st.divider()
    
    st.header("📚 Quick Guide")
    st.markdown("""
    **Gherkin Syntax:**
    - `Feature:` Description
    - `Scenario:` Test case name
    - `Given` Initial context
    - `When` Action/transformation
    - `Then` Expected outcome
    - `And` Additional conditions
    
    **Example Keywords:**
    - not null, unique, relationships
    - greater than, less than
    - no duplicates, accepted_values
    """)

# Main content
col1, col2 = st.columns([1, 1])

with col1:
    st.header("✍️ Write Gherkin Scenarios")
    
    gherkin_example = """Feature: Customer Data Quality
  
Scenario: Valid customer records
  Given a customers table with id, name, email
  When we load customer data
  Then column id should not be null
  And column id should be unique
  And column email should not be null

Scenario: Order amount validation
  Given an orders table with order_id and amount
  When we calculate total revenue
  Then column amount should be greater than 0
  And there should be no duplicates in order_id

Scenario: Status field validation
  Given a transactions table with status column
  When we process transactions
  Then column status should be one of 'pending', 'completed', 'failed'
"""
    
    gherkin_input = st.text_area(
        "Gherkin Scenarios",
        value=gherkin_example,
        height=400,
        help="Write your test scenarios in Gherkin syntax"
    )
    
    col_btn1, col_btn2 = st.columns(2)
    with col_btn1:
        if st.button("🔄 Parse & Generate", type="primary", use_container_width=True):
            scenarios = parse_gherkin(gherkin_input)
            if scenarios:
                st.session_state.gherkin_scenarios = scenarios
                
                # Generate all artifacts
                st.session_state.generated_tests = {}
                st.session_state.generated_models = {}
                
                for i, scenario in enumerate(scenarios):
                    test_key = f"scenario_{i}"
                    st.session_state.generated_tests[test_key] = {
                        'schema': generate_dbt_schema_test(scenario, model_name),
                        'singular': generate_dbt_singular_test(scenario, model_name),
                        'data_test': generate_data_test(scenario, model_name)
                    }
                    st.session_state.generated_models[test_key] = generate_dbt_model(
                        scenario, model_name, source_table
                    )
                
                st.success(f"✅ Generated {len(scenarios)} test scenarios!")
            else:
                st.error("❌ No valid scenarios found. Check your Gherkin syntax.")
    
    with col_btn2:
        if st.button("🗑️ Clear All", use_container_width=True):
            st.session_state.gherkin_scenarios = []
            st.session_state.generated_tests = {}
            st.session_state.generated_models = {}
            st.rerun()

with col2:
    st.header("📦 Generated Artifacts")
    
    if st.session_state.gherkin_scenarios:
        # Display parsed scenarios
        with st.expander("📋 Parsed Scenarios", expanded=False):
            for i, scenario in enumerate(st.session_state.gherkin_scenarios):
                st.markdown(f"**{i+1}. {scenario['name']}**")
                st.markdown(f"- **Given:** {', '.join(scenario['given'])}")
                st.markdown(f"- **When:** {', '.join(scenario['when'])}")
                st.markdown(f"- **Then:** {', '.join(scenario['then'])}")
                st.divider()
        
        # Tabs for different outputs
        tab1, tab2, tab3, tab4 = st.tabs(["📄 Schema Tests", "🧪 Singular Tests", "🔧 Models", "⚙️ Data Tests"])
        
        with tab1:
            st.markdown("### schema.yml - DBT Schema Tests")
            for key, tests in st.session_state.generated_tests.items():
                with st.expander(f"Test: {key}", expanded=True):
                    st.code(tests['schema'], language='yaml')
                    st.download_button(
                        "⬇️ Download",
                        tests['schema'],
                        file_name=f"schema_{key}.yml",
                        key=f"download_schema_{key}"
                    )
        
        with tab2:
            st.markdown("### Singular Tests (SQL)")
            for key, tests in st.session_state.generated_tests.items():
                with st.expander(f"Test: {key}", expanded=True):
                    st.code(tests['singular'], language='sql')
                    st.download_button(
                        "⬇️ Download",
                        tests['singular'],
                        file_name=f"test_{key}.sql",
                        key=f"download_singular_{key}"
                    )
        
        with tab3:
            st.markdown("### DBT Models")
            for key, model in st.session_state.generated_models.items():
                with st.expander(f"Model: {key}", expanded=True):
                    st.code(model, language='sql')
                    st.download_button(
                        "⬇️ Download",
                        model,
                        file_name=f"{model_name}_{key}.sql",
                        key=f"download_model_{key}"
                    )
        
        with tab4:
            st.markdown("### Custom Data Tests (Macros)")
            for key, tests in st.session_state.generated_tests.items():
                with st.expander(f"Data Test: {key}", expanded=True):
                    st.code(tests['data_test'], language='sql')
                    st.download_button(
                        "⬇️ Download",
                        tests['data_test'],
                        file_name=f"test_{key}_macro.sql",
                        key=f"download_datatest_{key}"
                    )
    else:
        st.info("👈 Write Gherkin scenarios and click 'Parse & Generate' to see results")

# Footer
st.divider()
st.markdown("""
<div style='text-align: center; color: #666; padding: 20px;'>
    <p>💡 <strong>Tip:</strong> Start with simple scenarios and iterate. Use specific keywords like 'not null', 'unique', 'greater than' for automatic test generation.</p>
    <p>📖 Learn more about <a href='https://docs.getdbt.com/docs/build/tests' target='_blank'>DBT Testing</a> and <a href='https://cucumber.io/docs/gherkin/' target='_blank'>Gherkin Syntax</a></p>
</div>
""", unsafe_allow_html=True)
