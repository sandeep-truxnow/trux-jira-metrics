# --- Jira Connection Function ---
import streamlit as st
import requests
from requests.auth import HTTPBasicAuth
from jira import JIRA
from jira.exceptions import JIRAError
from datetime import datetime, timedelta, date
from collections import OrderedDict
import re

STATUS_INPUT = "'Released', 'Closed'"
CYCLE_STATUSES = ["In Progress", "In Review", "Ready for Testing", "In Testing"]
WORKFLOW_STATUSES = [
    "To Do", "In Progress", "Paused", "In Review", "Ready for Testing",
    "In Testing", "QA Complete", "In UAT", "In UAT Testing",
    "Ready for Release", "Released", "Closed"
]

DETAILED_DURATIONS_DATA = OrderedDict([
    ("Current Sprint", "1"),
    ("Year to Date", "startOfYear()"),
    ("Current Month", "startOfMonth()"),
    ("Last Month", "startOfMonth(-1)"),
    ("Last 2 Months", "startOfMonth(-2)"),
    ("Custom Date Range", "customDateRange()")
])

if 'selected_custom_start_date' not in st.session_state: st.session_state.selected_custom_start_date = None
if 'selected_custom_end_date' not in st.session_state: st.session_state.selected_custom_end_date = None

def append_log(log_list, level, message):
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    log_list.append(f"[{timestamp}] [{level.upper()}] {message}")
    if level == "error" or level == "critical":
        st.error(f"[{timestamp}] {message}")
    elif level == "warning":
        st.warning(f"[{timestamp}] {message}")

@st.cache_resource
def connection_setup(jira_url, jira_email, jira_api_token, log_list):
    append_log(log_list, "info", f"JIRA Connect: Attempting connection to '{jira_url} for user {jira_email}...")
    jira_conn_details = ""

    if not jira_email or not jira_api_token:
        append_log(log_list, "error", "Please enter Username, and API Token.")
        st.stop()    
    
    with st.spinner("Connecting to Jira..."):
        jira_instance_conn = connect_to_jira_streamlit(jira_url, jira_email, jira_api_token, log_list)
    
        if jira_instance_conn:
            jira_conn_details = (jira_url, jira_email, jira_api_token)
            append_log(log_list, "info", "JIRA connection established for user lookup.")

        else:
            jira_conn_details = None
            append_log(log_list, "error", "Failed to connect to Jira. Please check your credentials.")

    # Only proceed if connection details exist
    if not jira_conn_details:
        append_log(log_list, "error", "ERROR No Jira connection established.")
        st.stop()
    
    return jira_conn_details

def show_sprint_name_start_date_and_end_date(selected_summary_duration_name, log_list):
    sprint_name = None
    sprint_start_date = None
    sprint_end_date = None

    # print(f"selected_summary_duration_name : {selected_summary_duration_name}")

    if selected_summary_duration_name == "Current Sprint":
        today_str = date.today().strftime("%Y-%m-%d")
        sprint_name, sprint_start_date, sprint_end_date = get_sprint_for_date(today_str)
    else:
        sprint_name = None
        match = re.search(r"\d{4}\.\d{2}", selected_summary_duration_name)
        if match:
            sprint_name = match.group()
        if sprint_name:
            sprint_start_date, sprint_end_date = get_sprint_dates_from_name(sprint_name)

    append_log(log_list, "info", f"Selected Sprint Name: {sprint_name}, sprint_start_date: {sprint_start_date}, sprint_end_date: {sprint_end_date}")

    return sprint_name, sprint_start_date, sprint_end_date

# def prepare_summary_jql_query(team_id, team_name, selected_summary_duration_name, log_list):
#     jql_query = ""

#     if selected_summary_duration_name == "Current Sprint":
#         jql_query = f"'Team[Team]' IN (\"{team_id}\") AND sprint in openSprints() AND issuetype NOT IN (Sub-task) ORDER BY KEY"

#     else:
#         sprint_name, sprint_start_date, sprint_end_date = show_sprint_name_start_date_and_end_date(selected_summary_duration_name, log_list)
#         jql_query = f"'Team[Team]' IN (\"{team_id}\") AND sprint = \"{team_name} {sprint_name}\" AND issuetype NOT IN (Sub-task) ORDER BY KEY"

#     if not jql_query:
#         append_log(log_list, "error", "Failed to generate JQL query. Please check your selections.")
#         st.stop()

#     append_log(log_list, "info", f"Generated JQL Query: {jql_query}")

#     return jql_query

def prepare_detailed_jql_query(selected_team_id, selected_detailed_duration_name, log_list):
    jql_query = ""

    if selected_detailed_duration_name == "Current Sprint":
        jql_query = f"'Team[Team]' = \"{selected_team_id}\" AND sprint in openSprints() AND issuetype NOT IN (Sub-task) ORDER BY KEY"
    else:
        # For all other durations, include status filter
        duration_func = DETAILED_DURATIONS_DATA.get(selected_detailed_duration_name, "")
        print(f"duration_func: {duration_func}")
        
        if duration_func == "customDateRange()":
            # Custom date range handling using session state dates
            start_date = st.session_state.selected_custom_start_date
            end_date = st.session_state.selected_custom_end_date
            
            if start_date and end_date:
                start_date_str = start_date.strftime("%Y-%m-%d")
                end_date_str = end_date.strftime("%Y-%m-%d")
                jql_query = (
                    f"'Team[Team]' = \"{selected_team_id}\" AND issuetype NOT IN (Sub-task) "
                    f"AND created >= '{start_date_str}' AND created <= '{end_date_str}' "
                    f"AND status IN ({STATUS_INPUT}) ORDER BY KEY"
                )
            else:
                append_log(log_list, "error", "Custom date range selected but start or end date is missing.")
                st.stop()
        else:
            jql_query = (
                f"'Team[Team]' = \"{selected_team_id}\" AND issuetype NOT IN (Sub-task) "
                f"AND created > {duration_func} AND status IN ({STATUS_INPUT}) ORDER BY KEY"
            )

    if not jql_query:
        append_log(log_list, "error", "Failed to generate JQL query. Please check your selections.")
        st.stop()

    append_log(log_list, "info", f"Generated JQL Query: {jql_query}")

    return jql_query


def connect_to_jira_streamlit(url, username, api_token, log_list):
    try:
        jira_options = {'server': url}
        jira = JIRA(options=jira_options, basic_auth=(username, api_token))
        return jira
    except Exception as e:
        append_log(log_list, "error", f"Error connecting to Jira: {e}")
        return None
    
# --- Data Fetching Functions (adapted for Streamlit caching and inputs) ---
@st.cache_data
def get_available_projects_streamlit(jira_url, jira_username, jira_api_token, log_list):
    jira_instance = connect_to_jira_streamlit(jira_url, jira_username, jira_api_token, log_list)
    if not jira_instance: return []
    try:
        projects = jira_instance.projects()
        project_list = [{'key': p.key, 'name': p.name} for p in projects]
        return project_list
    except JIRAError as e:
        append_log(log_list, "error", f"JIRA Connect: Error connecting to Jira: Status {e.status_code} - {e.text}")
        return None
    except Exception as e:
        append_log(log_list, "error", f"JIRA Connect: An unexpected error occurred during Jira connection: {e}")
        return None
    
@st.cache_data
def get_all_jira_users_streamlit(jira_url, jira_username, jira_api_token, log_list, filter_domain=None):
    append_log(log_list, "info", f"JIRA Users: Fetching all active Jira users from {jira_url}...")
    jira_instance = connect_to_jira_streamlit(jira_url, jira_username, jira_api_token, log_list)
    if not jira_instance: 
        append_log(log_list, "error", "JIRA Users: Jira instance not available to fetch users.")
        return {}
    
    all_users = {}
    start_at = 0
    max_results = 50 

    while True:
        users_page = fetch_users_page(jira_instance, start_at, max_results, log_list)
        if not users_page:
            break
        process_users_page(users_page, all_users, filter_domain)
        start_at += max_results
        if len(users_page) < max_results:
            break

    append_log(log_list, "info", f"JIRA Users: Fetched {len(all_users)} active human Jira users{get_filter_status_message(filter_domain)}.")
    return all_users


def fetch_users_page(jira_instance, start_at, max_results, log_list):
    try:
        return jira_instance.search_users(query='*', startAt=start_at, maxResults=max_results)
    except JIRAError as e:
        append_log(log_list, "error", f"JIRA Users: Error fetching users: Status {e.status_code} - {e.text}")
    except Exception as e:
        append_log(log_list, "error", "JIRA Users: An unexpected error occurred while fetching users.")
    return None


def process_users_page(users_page, all_users, filter_domain):
    for user in users_page:
        if hasattr(user, 'accountId') and user.accountId:
            email_lower = user.emailAddress.lower() if hasattr(user, 'emailAddress') else ''
            is_atlassian_user = determine_if_atlassian_user(user, email_lower)
            is_matching_domain = check_domain_match(email_lower, filter_domain)

            if is_atlassian_user and is_matching_domain:
                all_users[user.accountId] = {
                    'displayName': user.displayName if hasattr(user, 'displayName') else user.accountId,
                    'emailAddress': user.emailAddress if hasattr(user, 'emailAddress') else 'N/A'
                }


def determine_if_atlassian_user(user, email_lower):
    if hasattr(user, 'accountType') and user.accountType and user.accountType.lower() == 'atlassian':
        return True

    NON_HUMAN_KEYWORDS_FALLBACK = [
        '[APP]', 'automation', 'bot', 'service', 'plugin', 'jira-system',
        'addon', 'connect', 'integration', 'github', 'slack', 'webhook',
        'migrator', 'system', 'importer', 'syncer'
    ]
    display_name_lower = user.displayName.lower() if hasattr(user, 'displayName') else ''
    return not any(keyword in display_name_lower or keyword in email_lower for keyword in NON_HUMAN_KEYWORDS_FALLBACK)


def check_domain_match(email_lower, filter_domain):
    if not filter_domain:
        return True
    return email_lower.endswith(f"@{filter_domain.lower()}")


def get_filter_status_message(filter_domain):
    return f" (filtered by domain '{filter_domain}')" if filter_domain else ""

@st.cache_data
def get_custom_field_options_streamlit(jira_url, jira_username, jira_api_token, field_id, project_key, log_list, issue_type_name="Story"):
    jira_instance = connect_to_jira_streamlit(jira_url, jira_username, jira_api_token, log_list)
    if not jira_instance:
        append_log(log_list, "warn", "JIRA custom filed - Jira instance not available to fetch custom field options.")
        return []

    if not field_id:
        append_log(log_list, "warn", "Custom filed - Cannot fetch options: No field ID provided.")
        return []

    field_name, is_standard_select_list = get_field_info(jira_instance, field_id, log_list)
    if not field_name:
        return []

    if is_standard_select_list:
        options = fetch_options_from_createmeta(jira_instance, field_id, project_key, issue_type_name, field_name, log_list)
        if options:
            return options

    return fetch_options_from_jql(jira_instance, field_id, project_key, field_name, log_list)


def get_field_info(jira_instance, field_id, log_list):
    try:
        all_fields = jira_instance.fields()
        found_field_info = next((f for f in all_fields if f['id'] == field_id), None)

        if not found_field_info or 'schema' not in found_field_info:
            append_log(log_list, "warn", f"Custom field with ID '{field_id}' not found or lacks schema.")
            return None, False

        field_schema = found_field_info.get('schema')
        field_name = found_field_info.get('name', 'N/A')
        is_standard_select_list = field_schema.get('custom', '').startswith('com.atlassian.jira.plugin.system.customfieldtypes:select')
        return field_name, is_standard_select_list
    except Exception as e:
        append_log(log_list, "error", f"Error fetching field info for '{field_id}': {e}")
        return None, False


def fetch_options_from_createmeta(jira_instance, field_id, project_key, issue_type_name, field_name, log_list):
    try:
        createmeta = jira_instance.createmeta(projectKeys=project_key, issuetypeNames=issue_type_name, expand='projects.issuetypes.fields')
        if not createmeta or 'projects' not in createmeta or not createmeta['projects']:
            append_log(log_list, "warn", f"Project '{project_key}' metadata not found in createmeta.")
            return []

        project_meta = createmeta['projects'][0]
        issue_type_meta = next((it for it in project_meta.get('issuetypes', []) if it['name'] == issue_type_name), None)
        if not issue_type_meta or 'fields' not in issue_type_meta or field_id not in issue_type_meta['fields']:
            append_log(log_list, "warn", f"Field '{field_name}' ({field_id}) not found in createmeta for project '{project_key}'.")
            return []

        field_meta = issue_type_meta['fields'][field_id]
        allowed_values = field_meta.get('allowedValues', [])
        options = [opt.get('value') for opt in allowed_values if isinstance(opt, dict) and 'value' in opt]
        if options:
            append_log(log_list, "info", f"Fetched {len(options)} options for '{field_name}' ({field_id}) via createmeta.")
            return sorted(options)
        else:
            append_log(log_list, "warn", f"No 'allowedValues' found for '{field_name}' ({field_id}) in createmeta.")
            return []
    except Exception as e:
        append_log(log_list, "error", f"Error fetching options from createmeta for '{field_name}' ({field_id}): {e}")
        return []


def fetch_options_from_jql(jira_instance, field_id, project_key, field_name, log_list):
    try:
        jql_query = f'project = "{project_key}" AND "{field_name}" is not EMPTY'
        issues = jira_instance.search_issues(jql_query, fields=field_id, maxResults=100)

        unique_options = set()
        for issue in issues:
            field_value = getattr(issue.fields, field_id, None)
            if isinstance(field_value, dict) and 'value' in field_value:
                unique_options.add(field_value['value'])
            elif isinstance(field_value, list):
                unique_options.update(opt.get('value') for opt in field_value if isinstance(opt, dict) and 'value' in opt)

        options = sorted(unique_options)
        if options:
            append_log(log_list, "info", f"Fetched {len(options)} options for '{field_name}' ({field_id}) via JQL.")
        else:
           append_log(log_list, "warn", f"No options found for '{field_name}' ({field_id}) via JQL.")
        return options
    except Exception as e:
        append_log(log_list, "error", f"Error fetching options via JQL for '{field_name}' ({field_id}): {e}")
        return []

# === GET ISSUES FROM JQL ===
def get_issues_by_jql(jql, jira_url, username, api_token, log_list):
    auth = HTTPBasicAuth(username, api_token)
    if not jql.strip():
        append_log(log_list, "error", "ERROR JQL query cannot be empty.")
        st.stop()
    issue_keys = []
    start_at = 0
    max_results = 50
    while True:
        url = f"{jira_url}/rest/api/3/search"
        params = {
            "jql": jql,
            "fields": "key",
            "startAt": start_at,
            "maxResults": max_results
        }
        try:
            response = requests.get(url, auth=auth, params=params)
            response.raise_for_status()
            data = response.json()
            issues = data.get("issues", [])
            issue_keys.extend(issue['key'] for issue in issues)
            if len(issues) < max_results:
                break
            start_at += max_results
        except requests.exceptions.RequestException as e:
            append_log(log_list, "error", f"Network or API error during JQL search: {e}")
            st.stop()
        except Exception as e:
            append_log(log_list, "error", f"An unexpected error occurred during JQL search: {e}")
            st.stop()

    return issue_keys

# === FORMAT DURATION ===
# def format_duration(hours):
#     if hours is None: return "N/A"
#     if hours < 24: return f"{int(round(hours))} hrs"
#     days = int(hours // 24)
#     rem_hrs = int(round(hours % 24))
#     return f"{days} days" if rem_hrs == 0 else f"{days} days {rem_hrs} hrs"

def format_duration(hours):
    if hours is None:
        return "N/A"
    total_minutes = int(round(hours * 60))  # Convert to minutes
    days = total_minutes // (24 * 60)
    rem_minutes = total_minutes % (24 * 60)
    hrs = rem_minutes // 60
    mins = rem_minutes % 60

    parts = []
    if days > 0:
        parts.append(f"{days} days")
    if hrs > 0:
        parts.append(f"{hrs} hrs")
    if mins > 0 or not parts:
        parts.append(f"{mins} mins")

    return " ".join(parts)

def duration_to_hours(val):
    if not isinstance(val, str) or val.strip().upper() == "N/A":
        return None
    days = hrs = 0
    if m := re.search(r"(\d+)\s*days?", val):
        days = int(m.group(1))
    if m := re.search(r"(\d+)\s*hrs?", val):
        hrs = int(m.group(1))
    return days * 24 + hrs

# === GET ISSUE WITH CHANGELOG ===
def get_issue_changelog(issue_key, jira_url, username, api_token, log_list):
    auth = HTTPBasicAuth(username, api_token)
    url = f"{jira_url}/rest/api/3/issue/{issue_key}?expand=changelog"
    try:
        response = requests.get(url, auth=auth)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        append_log(log_list, "error", f"Network or API error fetching changelog for {issue_key}: {e}")
        raise
    except Exception as e:
        append_log(log_list, "error", f"An unexpected error occurred fetching changelog for {issue_key}: {e}")
        raise

def count_transitions(histories, from_status, to_status):
    count = 0
    for history in histories:
        for item in history['items']:
            if item['field'] == 'status':
                from_str = item['fromString']
                to_str = item['toString']
                
                if from_str == from_status and to_str == to_status:
                    count += 1

    return count

def seconds_to_dhm(seconds):
    days = seconds // 86400
    hours = (seconds % 86400) // 3600
    minutes = (seconds % 3600) // 60
    return f"{days} days {hours} hrs {minutes} mins"

def seconds_to_hm(seconds_str):
    try:
        seconds = int(seconds_str)
    except (ValueError, TypeError):
        return ""

    hours = seconds // 3600
    minutes = (seconds % 3600) // 60
    return f"{hours} hrs {minutes} mins"

# def seconds_to_hours(seconds_str):
#     try:
#         seconds = int(seconds_str)
#     except (ValueError, TypeError):
#         return ""

#     hours = round(seconds / 3600, 2)
#     return hours

def get_logged_time(histories):
    for history in histories:
        for item in history['items']:
            if item.get('field') == 'timespent':
                return int(item['to'])
    return 0

# === CALCULATE DURATIONS ===
def calculate_durations(transitions, created_time, issue_key, log_list):
    durations = {}
    status_times = {"To Do": created_time}

    for _, to_status, timestamp in transitions:
        if to_status and to_status not in status_times:
            status_times[to_status] = timestamp

    ordered_statuses = sorted(
        [status for status in WORKFLOW_STATUSES if status in status_times],
        key=lambda s: status_times[s]
    )

    for i in range(len(ordered_statuses) - 1):
        curr = ordered_statuses[i]
        nxt = ordered_statuses[i + 1]
        start_time = status_times[curr]
        end_time = status_times[nxt]
        diff = (end_time - start_time).total_seconds() / 3600.0
        if diff >= 0:
            durations[curr] = diff
        else:
            append_log(log_list, "warn", f"Negative duration between {curr} and {nxt} in issue {issue_key}.")

    if ordered_statuses:
        last_status = ordered_statuses[-1]
        if last_status not in durations:
            end_time = datetime.now(tz=status_times[last_status].tzinfo)
            diff = (end_time - status_times[last_status]).total_seconds() / 3600.0
            if diff >= 0:
                durations[last_status] = diff

    return durations

# === CALCULATE METRICS ===
def calculate_metrics(transitions, created_time):
    status_times = {}
    for _, to_status, timestamp in sorted(transitions, key=lambda x: x[2]):
        if to_status and to_status not in status_times:
            status_times[to_status] = timestamp

    lead_start = created_time
    lead_end = status_times.get("Released") or status_times.get("Closed")
    cycle_start = status_times.get("In Progress")
    cycle_end = status_times.get("QA Complete")

    lead_time = (lead_end - lead_start).total_seconds() / 3600.0 if lead_end else None
    cycle_time = (cycle_end - cycle_start).total_seconds() / 3600.0 if cycle_start and cycle_end else None
    return lead_time, cycle_time

# === PARSE CHANGELOG ===
def parse_changelog_from_history(changelog):
    transitions = []
    resolved_time = None

    for change in sorted(changelog, key=lambda x: x['created']):
        for item in change['items']:
            if item['field'] == 'status':
                from_status = item.get('fromString')
                to_status = item.get('toString')
                timestamp = datetime.strptime(change['created'], "%Y-%m-%dT%H:%M:%S.%f%z")
                
                transitions.append((from_status, to_status, timestamp))
                if to_status and to_status.lower() in ['qa complete', 'done', 'closed']:
                    resolved_time = timestamp
    return transitions, resolved_time

# === CALCULATE STATE DURATIONS ===
def calculate_state_durations(issue_key, issue_data, log_list):   
    changelog = issue_data['changelog']['histories']
    created_time = datetime.strptime(issue_data['fields']['created'], "%Y-%m-%dT%H:%M:%S.%f%z")
    transitions, resolved_time = parse_changelog_from_history(changelog)
    durations = calculate_durations(transitions, created_time, issue_key, log_list)
    lead_time, cycle_time = calculate_metrics(transitions, created_time)
    return {
        "lead_time_hours": lead_time,
        "cycle_time_hours": cycle_time,
        "durations_by_status_hours": dict(durations)
    }

def get_sprint_for_date(target_date, base_sprint="2025.12", base_start_date_str="2025-06-11", sprint_length_days=14):
    base_year, base_sprint_num = map(int, base_sprint.split("."))
    base_start_date = datetime.strptime(base_start_date_str, "%Y-%m-%d").date()
    target_date = datetime.strptime(target_date, "%Y-%m-%d").date()

    # Calculate days between target and base
    days_elapsed = (target_date - base_start_date).days
    sprint_offset = days_elapsed // sprint_length_days

    sprint_num = base_sprint_num + sprint_offset
    sprint_start_date = base_start_date + timedelta(days=sprint_offset * sprint_length_days)
    sprint_year = base_year

    # Adjust for year overflow
    while sprint_num > 52:
        sprint_num -= 52
        sprint_year += 1

    sprint_end_date = sprint_start_date + timedelta(days=sprint_length_days - 1)
    sprint_name = f"{sprint_year}.{sprint_num:02d}"

    return sprint_name, sprint_start_date, sprint_end_date

from datetime import datetime, date
if __name__ == "__main__":
    today_str = date.today().strftime("%Y-%m-%d")
    sprint_name, sprint_start_date, sprint_end_date = get_sprint_for_date(today_str)
    print(f"Today's sprint: {sprint_name} : sprint_start_date: {sprint_start_date} : sprint_end_date: {sprint_end_date}")

def get_sprint_dates_from_name(sprint_name, base_sprint="2025.12", base_start_date_str="2025-06-11", sprint_length_days=14):
    base_year, base_sprint_num = map(int, base_sprint.split("."))
    target_year, target_sprint_num = map(int, sprint_name.split("."))
    base_start_date = datetime.strptime(base_start_date_str, "%Y-%m-%d").date()

    # Calculate total number of sprints between base and target
    year_diff = target_year - base_year
    sprint_diff = (year_diff * 52) + (target_sprint_num - base_sprint_num)

    # Get sprint start and end dates
    sprint_start_date = base_start_date + timedelta(days=sprint_diff * sprint_length_days)
    sprint_end_date = sprint_start_date + timedelta(days=sprint_length_days - 1)

    return sprint_start_date, sprint_end_date


def get_previous_n_sprints(count, base_sprint="2025.12", base_start_date_str="2025-06-11", sprint_length_days=14):
    base_year, base_sprint_num = map(int, base_sprint.split("."))
    base_start_date = datetime.strptime(base_start_date_str, "%Y-%m-%d").date()
    today = datetime.today().date()

    # Calculate how many sprints have passed
    days_elapsed = (today - base_start_date).days
    sprint_offset = days_elapsed // sprint_length_days
    current_sprint_num = base_sprint_num + sprint_offset
    current_year = base_year

    # Adjust year if needed
    while current_sprint_num > 52:
        current_sprint_num -= 52
        current_year += 1

    # Get previous 'count' sprints
    result = []
    for i in range(count):
        sprint_num = current_sprint_num - i - 1
        year = current_year
        while sprint_num <= 0:
            sprint_num += 52
            year -= 1
        result.append(f"{year}.{sprint_num:02d}")
    return list(result)

def get_current_and_previous_sprints(team_name_for_sprint, base_sprint="2025.12", base_start_date_str="2025-06-11", sprint_length_days=14):
    base_year, base_sprint_num = map(int, base_sprint.split("."))
    base_start_date = datetime.strptime(base_start_date_str, "%Y-%m-%d").date()
    today = datetime.today().date()

    days_elapsed = (today - base_start_date).days
    if days_elapsed < 0:
        return (f"{team_name_for_sprint} {base_year}.{base_sprint_num:02d}", f"{team_name_for_sprint} {max(base_sprint_num - 1, 1):02d}")

    sprint_offset = days_elapsed // sprint_length_days
    current_sprint_num = base_sprint_num + sprint_offset
    current_year = base_year

    while current_sprint_num > 52:
        current_sprint_num -= 52
        current_year += 1
    
    previous_sprint_num = current_sprint_num - 1
    previous_sprint_year = current_year
    if previous_sprint_num <= 0:
        previous_sprint_num += 52
        previous_sprint_year -= 1

    return (
        f"{team_name_for_sprint} {current_year}.{current_sprint_num:02d}",
        f"{team_name_for_sprint} {previous_sprint_year}.{previous_sprint_num:02d}"
    )

def get_summary_issues_by_jql(jql, jira_url, username, api_token, log_list):
    auth = HTTPBasicAuth(username, api_token)
    if not jql.strip():
        log_list.append("ERROR JQL query cannot be empty.")
        return []
    
    issues = []
    start_at = 0
    max_results = 50
    
    while True:
        url = f"{jira_url}/rest/api/3/search"
        params = {
            "jql": jql,
            "fields": "key,fields",
            "startAt": start_at,
            "maxResults": max_results
        }
        try:
            response = requests.get(url, auth=auth, params=params)
            response.raise_for_status()
            data = response.json()
            batch_issues = data.get("issues", [])
            issues.extend(batch_issues)
            if len(batch_issues) < max_results:
                break
            start_at += max_results
        except requests.exceptions.RequestException as e:
            log_list.append(f"ERROR Network or API error during JQL search: {e}")
            break
        except Exception as e:
            log_list.append(f"ERROR An unexpected error occurred during JQL search: {e}")
            break
    return issues

def seconds_to_hours(seconds):
    if seconds is None or seconds == 0:
        return 0
    return round(seconds / 3600, 2)

def prepare_summary_jql_query(team_id, team_name, selected_duration_name, log_list):
    if selected_duration_name == "Current Sprint":
        jql = f'"Team" = "{team_id}" AND sprint in openSprints() AND issuetype NOT IN (Sub-task) ORDER BY KEY'
    elif selected_duration_name.startswith("Sprint "):
        sprint_name = selected_duration_name.replace("Sprint ", "")
        jql = f'"Team" = "{team_id}" AND sprint = "{team_name} {sprint_name}" AND issuetype NOT IN (Sub-task) ORDER BY KEY'
    else:
        jql = f'"Team" = "{team_id}" AND issuetype NOT IN (Sub-task) ORDER BY KEY'
    
    append_log(log_list, "info", f"Generated JQL for {team_name}: {jql}")
    return jql
