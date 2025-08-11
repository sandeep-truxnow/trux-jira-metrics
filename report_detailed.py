import pandas as pd
import numpy as np
import streamlit as st
from concurrent.futures import ThreadPoolExecutor
import requests
from requests.auth import HTTPBasicAuth
from datetime import datetime

from common import get_current_and_previous_sprints, format_duration, duration_to_hours, WORKFLOW_STATUSES, get_issue_changelog, calculate_state_durations, get_issues_by_jql, count_transitions, get_logged_time, seconds_to_hm

# === DETAILED COLUMN CONSTANTS ===
DETAILED_COLUMNS = {
    'KEY': 'Key',
    'TYPE': 'Type',
    'SUMMARY': 'Summary',
    'ASSIGNEE': 'Assignee',
    'STATUS': 'Status',
    'STORY_POINTS': 'Story Points',
    'SPRINTS': 'Sprints',
    'FAILED_QA_COUNT': 'Failed QA Count',
    'LOGGED_TIME': 'Logged Time',
    'CYCLE_TIME': 'Cycle Time',
    'LEAD_TIME': 'Lead Time'
}

# === GENERATE HEADERS ===
def generate_headers():
    return list(DETAILED_COLUMNS.values()) + WORKFLOW_STATUSES


# --- Custom Field IDs ---
CUSTOM_FIELD_ACCOUNT_ID = 'customfield_10267'
CUSTOM_FIELD_TEAM_ID = 'customfield_10001'
CUSTOM_FIELD_STORY_POINTS_ID = 'customfield_10014'

if 'generated_report_df_display' not in st.session_state: st.session_state.generated_report_df_display = None


def append_log(log_list, level, message):
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    log_list.append(f"[{timestamp}] [{level.upper()}] {message}")
    if level == "error" or level == "critical":
        st.error(f"[{timestamp}] {message}")
    elif level == "warning":
        st.warning(f"[{timestamp}] {message}")

# === REPORT GENERATOR ===
def collect_metrics_streamlit(issue_keys, jira_url, username, api_token, log_list):
    all_metrics = []
    auth = HTTPBasicAuth(username, api_token)

    def process_issue(key):
        try:
            issue_data = get_issue_changelog(key, jira_url, username, api_token, log_list)
            issue_meta = extract_issue_meta(key, issue_data, log_list)
            metrics = calculate_state_durations(key, issue_data, log_list)
            all_metrics.append((issue_meta, metrics))
        except requests.exceptions.RequestException as req_e:
            append_log(log_list, "error", f"Network error fetching issue {key}: {req_e}")
        except Exception as e:
            append_log(log_list, "error", f"Error processing issue {key}: {e}")

    with ThreadPoolExecutor(max_workers=20) as executor:
        list(executor.map(process_issue, issue_keys))
    return all_metrics

def generate_report_streamlit(issue_keys, jira_url, username, api_token, selected_team_name, log_list):
    append_log(log_list, "info", f"Collecting metrics for {len(issue_keys)} issues. This may take a while...")
    all_metrics = collect_metrics_streamlit(issue_keys, jira_url, username, api_token,  log_list)
    
    if not all_metrics:
        append_log(log_list, "warn", "No metrics collected. Report will be empty.")
        return None

    headers = generate_headers()
    data = [create_row(meta, metrics, selected_team_name) for meta, metrics in all_metrics]
    
    df = pd.DataFrame(data, columns=headers)
    
    append_log(log_list, "info", "Report data generated successfully!")
    return df

# === CREATE ROW FOR EXPORT ===
def create_row(meta, metrics, selected_team_name):
    durations = metrics['durations_by_status_hours']
    row = {
        **meta,
        DETAILED_COLUMNS['CYCLE_TIME']: format_duration(metrics['cycle_time_hours']),
        DETAILED_COLUMNS['LEAD_TIME']: format_duration(metrics['lead_time_hours']),
    }
    
    # --- Embed diamond differentiators in Sprints column in DataFrame ---
    if "Sprints" in meta and selected_team_name:
        original_sprints_str = meta["Sprints"]
        if original_sprints_str != "N/A":
            current_sprint_full, previous_sprint_full = get_current_and_previous_sprints(selected_team_name)
            
            sprint_values_in_cell = [s.strip() for s in original_sprints_str.split(",")]
            updated_sprints = []
            for sprint_text in sprint_values_in_cell:
                sprint_clean = sprint_text.replace("🔶", "").replace("🔷", "").strip()

                if sprint_clean == current_sprint_full:
                    updated_sprints.append(f"{sprint_clean} 🔶")
                elif sprint_clean == previous_sprint_full:
                    updated_sprints.append(f"{sprint_clean} 🔷")
                else:
                    updated_sprints.append(sprint_text)

            row["Sprints"] = ", ".join(updated_sprints)
    # --- End Fix ---

    for status in WORKFLOW_STATUSES:
        duration_in_hours = durations.get(status)
        row[status] = format_duration(duration_in_hours)
    return row

# --- Pandas Styling Functions for UI Display ---
def highlight_breached_durations_ui(s, cycle_threshold_hours, lead_threshold_hours):
    cycle_time_hours = duration_to_hours(s.get("Cycle Time"))
    lead_time_hours = duration_to_hours(s.get("Lead Time"))

    styles = [''] * len(s)

    try:
        if "Cycle Time" in s.index:
            cycle_col_idx = s.index.get_loc("Cycle Time")
            if cycle_time_hours is not None and cycle_time_hours > cycle_threshold_hours:
                styles[cycle_col_idx] = 'background-color: #FFD580' # Orange
        
        if "Lead Time" in s.index:
            lead_col_idx = s.index.get_loc("Lead Time")
            if lead_time_hours is not None and lead_time_hours > lead_threshold_hours:
                styles[lead_col_idx] = 'background-color: #FFD580' # Orange
    except KeyError:
        pass

    return styles

def apply_workflow_heatmap_ui(s):
    workflow_cols_in_row = [col for col in WORKFLOW_STATUSES if col not in {"Released", "Closed"} and col in s.index]

    row_durations_numerical = []
    for col_name in workflow_cols_in_row: # Use filtered subset
        hours = duration_to_hours(s.get(col_name))
        if hours is not None:
            row_durations_numerical.append(hours)
    
    if not row_durations_numerical:
        return [''] * len(s)

    min_val, max_val = min(row_durations_numerical), max(row_durations_numerical)
    delta = max_val - min_val if max_val != min_val else 1

    styles = [''] * len(s)

    for col_idx, col_name in enumerate(s.index):
        if col_name in workflow_cols_in_row: # Apply style only to filtered subset columns
            hours = duration_to_hours(s.get(col_name))
            if hours is not None:
                intensity = (hours - min_val) / delta
                hex_color = calculate_heatmap_color(intensity)
                styles[col_idx] = f'background-color: #{hex_color[2:]}'
    return styles

def apply_story_points_gradient_ui(s, min_sp_data, max_sp_data):
    styles = [''] * len(s)

    if max_sp_data == min_sp_data:
        single_color_val = max(0, min(1, (min_sp_data - 1) / 20.0))
        single_hex = calculate_heatmap_color_blue_gradient(single_color_val)
        return [f'background-color: #{single_hex[2:]}'] * len(s)

    delta_sp = max_sp_data - min_sp_data
    if delta_sp == 0: delta_sp = 1

    for idx, val in enumerate(s):
        try:
            sp_value = float(val)
            if sp_value >= 1 and sp_value == int(sp_value):
                normalized_value = (sp_value - min_sp_data) / delta_sp
                normalized_value = max(0, min(1, normalized_value))
                
                hex_color = calculate_heatmap_color_blue_gradient(normalized_value)
                styles[idx] = f'background-color: #{hex_color[2:]}'
        except ValueError:
            pass
    return styles

def calculate_heatmap_color(intensity):
    r = 255; g = int(200 - 120 * intensity); b = int(200 - 120 * intensity)
    r = max(0, min(255, r)); g = max(0, min(255, g)); b = max(0, min(255, b))
    return f"FF{r:02X}{g:02X}{b:02X}"

def calculate_heatmap_color_blue_gradient(intensity):
    r_start, g_start, b_start = (230, 240, 250)
    r_end, g_end, b_end = (21, 101, 192)

    r = int(r_start + (r_end - r_start) * intensity)
    g = int(g_start + (g_end - g_start) * intensity)
    b = int(b_start + (b_end - b_start) * intensity)
    
    return f"FF{r:02X}{g:02X}{b:02X}"


def generate_detailed_report(jira_conn_details, jql_query, selected_team_name, log_list):
    report_df_for_display = None
    auth_url, auth_username, auth_api_token = jira_conn_details

    issue_keys = get_issues_by_jql(jql_query, auth_url, auth_username, auth_api_token, log_list)

    if not issue_keys:
        append_log(log_list, "warn", "No issues found matching the JQL query. Report will be empty.")
        st.session_state.generated_report_df_display = None

        message = "This report is filtered and excludes sub-tasks. Includes only issues with status 'QA Complete', 'Released', or 'Closed'."
        
        # Show human-readable explanation
        if "openSprints()" in jql_query:
            st.warning(f"No issues found for **{selected_team_name}** team in the current active sprint. The team may not have any issues assigned for the current sprint. __{message}__.")
        elif "Custom Date Range" in jql_query:
            st.warning(f"No issues found for **{selected_team_name}** team in the selected date range. Try expanding the date range or check if the team has issues in this period. __{message}__.")
        else:
            st.warning(f"No issues found for **{selected_team_name}** team in the selected time period. The team may not have any issues assigned for this duration. __{message}__.")
    else:
        append_log(log_list, "info", f"Found {len(issue_keys)} issues matching the JQL query.")
        report_df_for_display = generate_report_streamlit(
            issue_keys, 
            auth_url, 
            auth_username, 
            auth_api_token, 
            selected_team_name,
            log_list
        )
        append_log(log_list, "info", "Report generated!")

    return report_df_for_display

def generated_report_df_display(df, cycle_threshold_hours, lead_threshold_hours, log_list):
    if df.empty or df.shape[0] == 0 or df.shape[1] == 0:
        log_list.append("WARN No data available to display in the report preview.")
        return

    st.markdown(f"**Total Records:** {df.shape[0]}")

    df_for_display_final = prepare_dataframe_for_display(df)
    styled_df = style_dataframe(
        df_for_display_final, cycle_threshold_hours, lead_threshold_hours
    )
    display_dataframe(styled_df)
    display_legend()
    display_column_definitions()    


def prepare_dataframe_for_display(df):
    df_for_display_final = df.copy()
    df_for_display_final.index = df_for_display_final.index + 1

    if "Story Points" in df_for_display_final.columns:
        df_for_display_final["Story Points"] = df_for_display_final["Story Points"].apply(
            lambda x: str(int(x)) if isinstance(x, (int, float)) and not pd.isna(x) else 'N/A'
        )
    return df_for_display_final


def style_dataframe(df, cycle_threshold_hours, lead_threshold_hours):
    styled_df = df.style

    styled_df = styled_df.apply(
        lambda s: highlight_breached_durations_ui(s, cycle_threshold_hours, lead_threshold_hours), axis=1
    )

    workflow_cols_present = [col for col in WORKFLOW_STATUSES if col in df.columns]
    if workflow_cols_present:
        styled_df = styled_df.apply(
            lambda row: apply_workflow_heatmap_ui(row), axis=1, subset=workflow_cols_present
        )

    if "Story Points" in df.columns:
        styled_df = apply_story_points_gradient(styled_df, df["Story Points"])
    return styled_df


def apply_story_points_gradient(styled_df, story_points_column):
    temp_sp_series = story_points_column.apply(
        lambda x: float(str(x)) if str(x).replace('.', '', 1).isdigit() else np.nan
    )
    numerical_sp_values = temp_sp_series.dropna()

    if not numerical_sp_values.empty:
        min_sp_data = numerical_sp_values.min()
        max_sp_data = numerical_sp_values.max()

        if max_sp_data == min_sp_data:
            single_color_val = max(0, min(1, (min_sp_data - 1) / 20.0))
            single_hex = calculate_heatmap_color_blue_gradient(single_color_val)
            styled_df = styled_df.apply(
                lambda s_col: [f'background-color: #{single_hex[2:]}'] * len(s_col),
                subset=["Story Points"]
            )
        else:
            styled_df = styled_df.apply(
                lambda s_col: apply_story_points_gradient_ui(s_col, min_sp_data, max_sp_data),
                subset=["Story Points"]
            )
    return styled_df


def display_dataframe(styled_df):
    st.dataframe(styled_df, use_container_width=True, hide_index=True, column_config={
        "Key": st.column_config.Column("Key", width="small", help="Jira Issue Key", pinned="left"),
        "Type": st.column_config.Column("Type", width="small", help="Jira Issue Type")
    })


def display_column_definitions():
    with st.expander("📋 Column Definitions", expanded=False):
        st.markdown("""
        - **Key**: Jira issue key/identifier
        - **Type**: Issue type (Story, Bug, Task, etc.)
        - **Summary**: Brief description of the issue
        - **Assignee**: Person assigned to work on the issue
        - **Status**: Current status of the issue
        - **Story Points**: Estimated effort/complexity points
        - **Sprints**: Sprint(s) the issue has been part of (🔶 = current, 🔷 = previous)
        - **Failed QA Count**: Number of times issue was rejected from "In Testing" status
        - **Logged Time**: Total time logged on the issue
        - **Cycle Time**: Time from "In Progress" to "QA Complete" (development time)
        - **Lead Time**: Time from issue creation to "Done/Closed/Released" (total delivery time)
        - **Workflow Status Columns**: Time spent in each workflow status (To Do, In Progress, etc.)
        """)

def display_legend():
    with st.expander("🎨 Legend", expanded=False):
        st.markdown(
            """
            <style>
            .legend-item {
                display: flex;
                align-items: center;
                margin-bottom: 5px;
            }
            .color-box {
                width: 20px;
                height: 20px;
                border: 1px solid #ccc;
                margin-right: 10px;
            }
            </style>
            <div class="legend-item">
                <div class="color-box" style="background-color: #FFD580;"></div>
                <span>Cycle Time / Lead Time > threshold</span>
            </div>
            <div class="legend-item">
                <div class="color-box" style="background-color: #1565C0;"></div>
                <span>Story Points: Light → Dark Blue (low → high)</span>
            </div>
            <div class="legend-item">
                <div class="color-box" style="background-color: #FF6666;"></div>
                <span>Workflow: Light → Dark Red (per row, if breached)</span>
            </div>
            """, unsafe_allow_html=True
        )

# === EXTRACT ISSUE META ===
def extract_issue_meta(key, issue_data, log_list):
    fields = issue_data['fields']
    if not fields:
        append_log(log_list, "error", f"No fields found for issue {key}.")
        return {}

    sprint_str = extract_sprint_string(fields)
    story_points_value = extract_story_points(fields)
    failed_qa_count = count_transitions(issue_data['changelog']['histories'], "In Testing", "Rejected") or 0
    logged_time_in_seconds = get_logged_time(issue_data['changelog']['histories'])

    return {
        DETAILED_COLUMNS['KEY']: key,
        DETAILED_COLUMNS['TYPE']: fields['issuetype']['name'],
        DETAILED_COLUMNS['SUMMARY']: fields['summary'],
        DETAILED_COLUMNS['ASSIGNEE']: fields['assignee']['displayName'] if fields['assignee'] else "Unassigned",
        DETAILED_COLUMNS['STATUS']: fields['status']['name'],
        DETAILED_COLUMNS['STORY_POINTS']: story_points_value,
        DETAILED_COLUMNS['SPRINTS']: sprint_str,
        DETAILED_COLUMNS['FAILED_QA_COUNT']: failed_qa_count,
        DETAILED_COLUMNS['LOGGED_TIME']: seconds_to_hm(logged_time_in_seconds),
    }

def extract_sprint_string(fields):
    sprints_field = fields.get('customfield_10010')
    if not isinstance(sprints_field, list):
        return "N/A"

    sprint_names = []
    sprints_field.sort(key=lambda x: x.get('id', 0), reverse=True)
    for sprint in sprints_field:
        if isinstance(sprint, dict) and 'name' in sprint:
            sprint_names.append(sprint['name'])
    return ", ".join(sprint_names) if sprint_names else "N/A"

def extract_story_points(fields):
    story_points_value = fields.get(CUSTOM_FIELD_STORY_POINTS_ID, None)
    if story_points_value is None or (isinstance(story_points_value, float) and np.isnan(story_points_value)):
        return "N/A"
    if isinstance(story_points_value, (int, float)):
        return int(story_points_value)
    if isinstance(story_points_value, str) and story_points_value.replace('.', '', 1).isdigit():
        try:
            return int(float(story_points_value))
        except ValueError:
            return "N/A"
    return str(story_points_value)
