import os  
import glob 
import yaml 
import pandas as pd 
from typing import TypedDict, List, Dict, Any
from langgraph.graph import StateGraph, END


# State : Memory of Agent 
class DQ_Agent(TypedDict):
    rules: Dict[str, Any]
    files: List[str]
    validation_results: Dict[str, Any]
    approval_status: str # Whether data is "Pass" or "Fail"
    report: str 


# Tools for Agent

def load_rules(rules_path: str) -> Dict:
    """
    Load validation rules from a YAML file.
    """
    with open(rules_path, 'r') as file:
        return yaml.safe_load(file)

def scan_knowledge_base(folder_path: str) -> List[str]:
    """Scan a folder for relevant files (CSV, JSON, PDF, YAML) and return their paths."""
    file_extensions = ["*.csv", "*.json", "*.pdf", "*.yaml"]
    files = []
    for i in file_extensions:
        files.extend(glob.glob(os.path.join(folder_path, i)))
    return files 

def validate_csv(file_path: str, rules: List[Dict]) -> Dict:
    """Validate a CSV file against a set of rules and return the results."""
    results = {
        "file": file_path,
        "failed_rules": [],
        "approval_status": "Pass",    # or "Fail"
    }

    try:
        df = pd.read_csv(file_path)

    except Exception as e:
        print(f"Error reading {file_path}: {e}")
        results["approval_status"] = "Fail"
        return results
    
    for rule in rules:
        rule_id = rule.get("rule_id")
        check = rule.get("check")
        description = rule.get("description")

        rule_failure_reason = None

        if check == "file_not_empty":
            if df.empty:
                rule_failure_reason = "File is empty"

        elif check == "required_columns":
            required = rule.get("columns", [])
            missing = [col for col in required if col not in df.columns]
            if missing:
                rule_failure_reason = f"Missing columns: {', '.join(missing)}"
        
        elif check == "correct_empid_dtype":
            columns = rule.get("columns", {})
            for col_name, expected_dtype in columns.items():
                if col_name in df.columns:
                    if expected_dtype == "integer":
                        non_int = pd.to_numeric(df[col_name], errors='coerce')
                        invalid_rows = non_int.isna() | (non_int != non_int.astype(int, errors='ignore'))
                        if invalid_rows.any():
                            rule_failure_reason = f"Column '{col_name}' contains non-integer values"
                else:
                    rule_failure_reason = f"Column '{col_name}' not found for dtype check"

        elif check == "no_duplicates":
            dup_columns = rule.get("columns", [])
            for col_name in dup_columns:
                if col_name in df.columns:
                    if df[col_name].duplicated().any():
                        rule_failure_reason = f"Duplicate values found in column '{col_name}'"

    
        if rule_failure_reason:
            results["failed_rules"].append({
                "rule_id": rule_id,
                "description": description,
                "reason": rule_failure_reason
            })
            results["approval_status"] = "Fail"

    return results

def send_email_report():
    """Send an email report to stakeholders with the validation results and next steps."""
    return "Report sent via email using tool"

# LangGraph Nodes 

def node_load_rules(state: DQ_Agent) -> DQ_Agent:
    print("\nLoading rules from data_validator_rules.yaml ...")
    with open("data_validator_rules.yaml", "r") as f:
        raw_config = yaml.safe_load(f)
    # Extract the actual rules list from the nested YAML structure
    state["rules"] = raw_config.get("rules", [])
    print("Rules loaded.")
    
    print("\n Scanning the knowledge base/* ...")
    file_extensions = ["*.csv", "*.json", "*.pdf", "*.yaml"]
    files = []
    for i in file_extensions:
        files.extend(glob.glob(os.path.join("knowledge_base", i)))

    state["files"] = files 
    print(f"Found {len(files)} files: {files}")
    return state

def node_validate_data(state: DQ_Agent) -> DQ_Agent:
    print("\n Validating data files against rules...")

    all_results = []
    kb_files = state.get("files", [])
    rule_groups = state.get("rules", [])

    for file in kb_files:
        if file.endswith(".csv"):
            # Find matching rule group for CSV files
            applicable_rules = []
            for group in rule_groups:
                import fnmatch
                if fnmatch.fnmatch(os.path.basename(file), group.get("applicable_files", "")):
                    applicable_rules = group.get("rules", [])
                    break
            results = validate_csv(file, applicable_rules)
            all_results.append(results)
        else:
            print(f"Skipping unsupported file type: {file}")

    state["validation_results"] = all_results
    # Determine overall approval status
    approval_status = "Pass"
    for result in all_results:
        if result.get("approval_status") == "Fail":
            approval_status = "Fail"
            break
    state["approval_status"] = approval_status
    
    return state

def node_send_email_report(state: DQ_Agent) -> DQ_Agent:
    state["report"] = "Report sent via email"
    return state

def node_data_approve_gateway(state: DQ_Agent) -> DQ_Agent:
    validation_results = state.get("validation_results", [])
    approval_status = "Pass"

    for result in validation_results:
        if result.get("approval_status") == "Fail":
            approval_status = "Fail"
            break

    state["approval_status"] = approval_status
    return state
    

def condition_func(state):
    status = state.get("approval_status", "Pass")
    print(f"Condition status: {status}")
    return status

# Build Graph 

workflow_graph = StateGraph(DQ_Agent)

# register nodes
workflow_graph.add_node("node_load_rules", node_load_rules)
workflow_graph.add_node("node_validate_data", node_validate_data)
workflow_graph.add_node("node_data_approve_gateway", node_data_approve_gateway)
workflow_graph.add_node("node_send_email_report", node_send_email_report) 

# Set the entry point for the graph
workflow_graph.set_entry_point("node_load_rules")

workflow_graph.add_edge("node_load_rules", "node_validate_data")
# workflow_graph.add_edge("node_validate_data", "node_data_approve_gateway")

workflow_graph.add_conditional_edges("node_validate_data", condition_func, {
    "Pass": "node_data_approve_gateway",
    "Fail": "node_send_email_report"
})

workflow_graph.add_edge("node_data_approve_gateway", END)
workflow_graph.add_edge("node_send_email_report", END)

compiled_graph = workflow_graph.compile()

if __name__ == "__main__":
    # Initialization 
    with open("data_validator_rules.yaml", "r") as f:
        raw_config = yaml.safe_load(f)
    initial_state = DQ_Agent(
        rules=raw_config.get("rules", []),
        files=[],
        validation_results={},
        approval_status="",
        report=""
    )

    final_answer = compiled_graph.invoke(initial_state)
    print("\n" + "*" * 55)
    print("Final State of DQ Agent:", final_answer)

