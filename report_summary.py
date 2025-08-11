import pandas as pd
import numpy as np
import streamlit as st
from concurrent.futures import ThreadPoolExecutor, as_completed
import requests
from requests.auth import HTTPBasicAuth
from datetime import datetime, timezone

from common import seconds_to_hours, get_summary_issues_by_jql, prepare_summary_jql_query, get_issue_changelog, get_logged_time, show_sprint_name_start_date_and_end_date, get_logged_time_per_sprint, get_logged_time_per_sprint, count_transitions


# === SUMMARY COLUMN CONSTANTS ===
SUMMARY_COLUMNS = {
    'TEAMS': 'Teams',
    'TOTAL_ISSUES': 'Total Issues', 
    'STORY_POINTS': 'Story Points',
    'ISSUES_COMPLETED': 'Issues Completed',
    'PERCENT_COMPLETED': 'Completion %',
    'SPRINT_HOURS': 'Sprint Hrs',
    'ALL_TIME_HOURS': 'All Time Hrs',
    'BUGS': 'Bugs',
    'FAILED_QA_COUNT': 'Failed QA Count',
    'SPILLOVER_ISSUES': 'Spillover Issues',
    'SPILLOVER_POINTS': 'Spillover Story Points',
    'AVG_COMPLETION_DAYS': 'Avg Completion Days',
    'AVG_SPRINTS_STORY': 'Avg Sprints/Story',
    'BUGS_SPRINT_HOURS': 'Bugs Sprint Hrs',
    'BUGS_ALL_TIME_HOURS': 'Bugs All Time Hrs',
    'SCOPE_CHANGES': 'Scope Changes (Issues)'
}

# === SCOPE CHANGE CONFIGURATION ===
SCOPE_CHANGE_GRACE_PERIOD_HOURS = 48  # Hours after sprint start to ignore scope changes

# === GENERATE HEADERS ===
def generate_headers():
    return list(SUMMARY_COLUMNS.values())


# --- Custom Field IDs ---
CUSTOM_FIELD_TEAM_ID = 'customfield_10001'
CUSTOM_FIELD_STORY_POINTS_ID = 'customfield_10014'



if 'generated_summary_report_df_display' not in st.session_state: st.session_state.generated_summary_report_df_display = None



def append_log(log_list, level, message):
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    log_list.append(f"[{timestamp}] [{level.upper()}] {message}")
    if level == "error" or level == "critical":
        st.error(f"[{timestamp}] {message}")
    elif level == "warning":
        st.warning(f"[{timestamp}] {message}")


from concurrent.futures import ThreadPoolExecutor, as_completed

def get_team_name_by_id(team_id, teams_data):
    return next((name for name, tid in teams_data.items() if tid == team_id), None)

def generate_summary_report(team_ids, jira_conn_details, selected_summary_duration_name, teams_data, log_list):
    jira_url, jira_username, jira_api_token = jira_conn_details
    team_metrics = {}

    def process_team(team_id):
        team_name = get_team_name_by_id(team_id, teams_data)
        jql = prepare_summary_jql_query(team_id, team_name, selected_summary_duration_name, log_list)
        
        sprint_name, sprint_start_date, sprint_end_date = show_sprint_name_start_date_and_end_date(selected_summary_duration_name, log_list)
        
        # Try to get actual sprint dates from JIRA
        from common import get_actual_sprint_dates_from_jira
        actual_start_dt, actual_end_dt = get_actual_sprint_dates_from_jira(jira_url, jira_username, jira_api_token, sprint_name, team_name, log_list)
        if actual_start_dt and actual_end_dt:
            # Use actual datetime objects from JIRA (already in NY timezone)
            sprint_start_datetime = actual_start_dt
            sprint_start_date = actual_start_dt.date()
            sprint_end_date = actual_end_dt.date()
            append_log(log_list, "info", f"Using actual JIRA sprint datetimes for {team_name}")
        else:
            # Fallback to calculated dates converted to America/New_York timezone
            from zoneinfo import ZoneInfo
            sprint_start_datetime = datetime.combine(sprint_start_date, datetime.min.time()).replace(tzinfo=ZoneInfo('America/New_York'))
        
        append_log(log_list, "info", f"==> {team_name} {selected_summary_duration_name} sprint_start_date = {sprint_start_date}, sprint_start_datetime = {sprint_start_datetime} (America/New_York), sprint_end_date = {sprint_end_date}")
       
        issues = get_summary_issues_by_jql(jql, jira_url, jira_username, jira_api_token, log_list)

        if not issues:
            append_log(log_list, "warn", f"No issues found for team {team_name}. Report will be empty.")
            return team_id, None
        else:
            append_log(log_list, "info", f"Found {len(issues)} issues for team {team_name}.")

        all_metrics = generate_summary_report_streamlit(
            team_name, issues, jira_url, jira_username, jira_api_token, selected_summary_duration_name, sprint_start_datetime, sprint_end_date, log_list
        )
            
        total_issues = len(all_metrics)
        total_story_points = sum(issue['story_points'] for issue in all_metrics)
        total_issues_closed = sum(issue['issues_closed'] for issue in all_metrics)
        total_sprint_hours = sum(issue['sprint_hours'] for issue in all_metrics)
        total_all_time_hours = sum(issue['all_time_hours'] for issue in all_metrics)
        total_failed_qa_count = sum(issue['failed_qa_count'] for issue in all_metrics)
        total_bugs = sum(issue['bug_count'] for issue in all_metrics)
        total_spillover_issues = sum(issue['spillover_issues'] for issue in all_metrics)
        total_spillover_points = sum(issue['spillover_story_points'] for issue in all_metrics)

        bugs_hours_in_current_sprint = sum(issue['bugs_hours_in_current_sprint'] for issue in all_metrics)
        total_all_time_bugs_hours = sum(issue['total_all_time_bugs_hours'] for issue in all_metrics)
        total_added_issues = sum(issue['added_to_sprint'] for issue in all_metrics)
        total_removed_issues = sum(issue['removed_from_sprint'] for issue in all_metrics)

        # Calculate average completion time for completed stories
        completed_stories = [issue for issue in all_metrics if issue['issues_closed'] > 0 and issue['completion_time_days'] > 0]
        avg_completion_days = sum(issue['completion_time_days'] for issue in completed_stories) / len(completed_stories) if completed_stories else 0
        
        # Calculate average number of sprints per story
        avg_sprints_per_story = sum(issue['sprint_count'] for issue in all_metrics) / len(all_metrics) if all_metrics else 0
        
        percent_work_complete = round((total_issues_closed / total_issues) * 100, 2) if total_issues else 0

        """
            Breaking it down:

            A. completed_stories - First, it filters to only include completed issues:
                completed_stories = [issue for issue in all_metrics if issue['issues_closed'] > 0 and issue['completion_time_days'] > 0]
                - Only issues that are actually completed ( issues_closed > 0)
                - Only issues that have a valid completion time ( completion_time_days > 0)

                sum(issue['completion_time_days'] for issue in completed_stories) - Adds up completion days for completed stories only
                    - Each issue has completion_time_days = days from creation to completion
                    - For example: Story A (5 days) + Story B (12 days) + Story C (8 days) = 25 total days

                / len(completed_stories) - Divides by the number of completed stories
                    - len(completed_stories) gives count of completed issues (e.g., 3 completed stories)
                if completed_stories else 0 - Safety check to avoid division by zero
                    - If no stories are completed, return 0

                Example:
                    - Team has 5 total issues, but only 3 are completed:
                        Story A: 5 days to complete
                        Story B: 12 days to complete
                        Story C: 8 days to complete
                        Story D: Not completed (ignored)
                        Story E: Not completed (ignored)

                        Calculation: (5 + 12 + 8) ÷ 3 = 25 ÷ 3 = 8.3 average completion days

                This metric shows how long it typically takes the team to complete stories from creation to done, helping identify delivery speed and potential bottlenecks.

            ====================================================================================================================================

            B. avg_sprints_per_story
                a. sum(issue['sprint_count'] for issue in all_metrics) - Adds up the total number of sprints across all issues
                    - Each issue has a sprint_count field indicating how many sprints that issue has been part of
                    - For example: Issue A (2 sprints) + Issue B (1 sprint) + Issue C (3 sprints) = 6 total sprints
                b. / len(all_metrics) - Divides by the total number of issues
                    - len(all_metrics) gives the count of issues (e.g., 3 issues)
                c. if all_metrics else 0 - Safety check to avoid division by zero
                    - If there are no issues, return 0 instead of crashing

                Example:
                Team has 3 issues:
                    - Issue A: 2 sprints
                    - Issue B: 1 sprint
                    - Issue C: 3 sprints
                Calculation: (2 + 1 + 3) ÷ 3 = 6 ÷ 3 = 2.0 average sprints per story
        """


        return team_id, {
            SUMMARY_COLUMNS['TOTAL_ISSUES']: total_issues,
            SUMMARY_COLUMNS['STORY_POINTS']: total_story_points,
            SUMMARY_COLUMNS['ISSUES_COMPLETED']: total_issues_closed,
            SUMMARY_COLUMNS['PERCENT_COMPLETED']: percent_work_complete,
            SUMMARY_COLUMNS['SPRINT_HOURS']: seconds_to_hours(total_sprint_hours),
            SUMMARY_COLUMNS['ALL_TIME_HOURS']: seconds_to_hours(total_all_time_hours),
            SUMMARY_COLUMNS['BUGS']: total_bugs,
            SUMMARY_COLUMNS['FAILED_QA_COUNT']: total_failed_qa_count,
            SUMMARY_COLUMNS['SPILLOVER_ISSUES']: total_spillover_issues,
            SUMMARY_COLUMNS['SPILLOVER_POINTS']: total_spillover_points,
            SUMMARY_COLUMNS['AVG_COMPLETION_DAYS']: round(avg_completion_days, 1),
            SUMMARY_COLUMNS['AVG_SPRINTS_STORY']: round(avg_sprints_per_story, 1),
            SUMMARY_COLUMNS['BUGS_SPRINT_HOURS']: seconds_to_hours(bugs_hours_in_current_sprint),
            SUMMARY_COLUMNS['BUGS_ALL_TIME_HOURS']: seconds_to_hours(total_all_time_bugs_hours),
            SUMMARY_COLUMNS['SCOPE_CHANGES']: f"+{total_added_issues}/-{total_removed_issues}"
        }

    # Run all teams in parallel
    import streamlit as st
    from streamlit.runtime.scriptrunner import add_script_run_ctx
    
    with ThreadPoolExecutor(max_workers=min(10, len(team_ids))) as executor:
        futures = {}
        for team_id in team_ids:
            future = executor.submit(process_team, team_id)
            add_script_run_ctx(future)
            futures[future] = team_id

        for future in as_completed(futures):
            team_id, result = future.result()
            if result:
                team_metrics[team_id] = result

    append_log(log_list, "info", "Report generated!")
    return team_metrics

def get_issue_summary_by_jql(jql, jira_url, username, api_token, log_list):
    auth = HTTPBasicAuth(username, api_token)
    if not jql.strip():
        append_log(log_list, "error", "ERROR JQL query cannot be empty.")
        st.stop()
    issues = []
    start_at = 0
    max_results = 50
    total_issues = 0
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
            # print(f"data: {data}")
            total_issues = data.get("total", 0)
            append_log(log_list, "info", f"Total issues found: {total_issues}")
            if total_issues == 0:
                append_log(log_list, "warn", "No issues found matching the JQL query.")
                st.stop()
            
            issues = data.get("issues", [])
            # issue_keys.extend(issue['key'] for issue in issues)
            if len(issues) < max_results:
                break
            start_at += max_results
        except requests.exceptions.RequestException as e:
            append_log(log_list, "error", f"Network or API error during JQL search: {e}")
            st.stop()
        except Exception as e:
            append_log(log_list, "error", f"An unexpected error occurred during JQL search: {e}")
            st.stop()

    
    return issues

def generated_summary_report_df_display():
    return ""

def generate_summary_report_streamlit(team_name, issues, jira_url, username, api_token, selected_summary_duration_name, sprint_start_date, sprint_end_date, log_list):
    append_log(log_list, "info", f"Collecting metrics for {len(issues)} issues. This may take a while...")
    all_metrics = collect_metrics_streamlit(issues, jira_url, username, api_token, selected_summary_duration_name, team_name, sprint_start_date, sprint_end_date, log_list)
    
    if not all_metrics:
        append_log(log_list, "warn", "No metrics collected. Report will be empty.")
        return None

    append_log(log_list, "info", f"{team_name} metrics generated successfully!")
    return all_metrics

def collect_metrics_streamlit(issues, jira_url, username, api_token, selected_summary_duration_name, team_name, sprint_start_date, sprint_end_date, log_list):
    all_metrics = []

    def process_issue(issue):
        try:
            issue_key = issue.get("key", "")
            issue_data = get_issue_changelog(issue_key, jira_url, username, api_token, log_list)
            return extract_issue_meta(issue, issue_data, selected_summary_duration_name, team_name, sprint_start_date, sprint_end_date, log_list)
        except Exception as e:
            append_log(log_list, "error", f"Error processing issue {issue.get('key', 'unknown')}: {e}")
            return None

    # Process issues in parallel with increased workers
    import streamlit as st
    from streamlit.runtime.scriptrunner import add_script_run_ctx
    
    with ThreadPoolExecutor(max_workers=20) as executor:
        futures = []
        for issue in issues:
            future = executor.submit(process_issue, issue)
            add_script_run_ctx(future)
            futures.append(future)
        
        for future in as_completed(futures):
            result = future.result()
            if result:
                all_metrics.append(result)

    return all_metrics


def generated_summary_report_df_display(team_metrics, teams_data):
    # Convert to displayable list of rows
    rows = []
    TEAM_ID_TO_NAME = {v: k for k, v in teams_data.items()}

    for team_id, metrics in team_metrics.items():
        team_name = TEAM_ID_TO_NAME.get(team_id, team_id)
        
        rows.append([
            team_name,
            metrics.get(SUMMARY_COLUMNS['TOTAL_ISSUES'], 0),
            metrics.get(SUMMARY_COLUMNS['STORY_POINTS'], 0),
            metrics.get(SUMMARY_COLUMNS['ISSUES_COMPLETED'], 0),
            metrics.get(SUMMARY_COLUMNS['PERCENT_COMPLETED'], 0.0),
            metrics.get(SUMMARY_COLUMNS['SPRINT_HOURS'], 0.0),
            metrics.get(SUMMARY_COLUMNS['ALL_TIME_HOURS'], 0.0),
            metrics.get(SUMMARY_COLUMNS['BUGS'], 0),
            metrics.get(SUMMARY_COLUMNS['FAILED_QA_COUNT'], 0),
            metrics.get(SUMMARY_COLUMNS['SPILLOVER_ISSUES'], 0),
            metrics.get(SUMMARY_COLUMNS['SPILLOVER_POINTS'], 0),
            metrics.get(SUMMARY_COLUMNS['AVG_COMPLETION_DAYS'], 0.0),
            metrics.get(SUMMARY_COLUMNS['AVG_SPRINTS_STORY'], 0.0),
            metrics.get(SUMMARY_COLUMNS['BUGS_SPRINT_HOURS'], 0.0),
            metrics.get(SUMMARY_COLUMNS['BUGS_ALL_TIME_HOURS'], 0.0),
            metrics.get(SUMMARY_COLUMNS['SCOPE_CHANGES'], "+0/-0")
        ])

    df = pd.DataFrame(rows, columns=generate_headers())

    # Keep % Complete as numeric for proper alignment

    # add total to each column, for the Teams Column, show label as Total
    total_row = df.select_dtypes(include='number').sum(numeric_only=True)
    
    # Calculate average for Completion % and keep as numeric
    completion_percentages = [row[4] for row in rows if rows]
    avg_percent = np.mean(completion_percentages) if completion_percentages else 0
    total_row["Completion %"] = avg_percent

    # Calculate sum of scope changes for Grand Total
    total_added = 0
    total_removed = 0
    for team_id, metrics in team_metrics.items():
        scope_changes = metrics.get(SUMMARY_COLUMNS['SCOPE_CHANGES'], "+0/-0")
        # Parse the "+X/-Y" format
        if "+" in scope_changes and "/-" in scope_changes:
            parts = scope_changes.split("/-")
            added = int(parts[0].replace("+", ""))
            removed = int(parts[1])
            total_added += added
            total_removed += removed
    
    total_row["Scope Changes (Issues)"] = f"+{total_added}/-{total_removed}"

    # Add 'Teams' label
    total_row["Teams"] = "Grand Total"

    # Append the row as the last row
    df.loc[len(df)] = total_row

    return df
    

def _get_story_points(fields):
    story_points = fields.get(CUSTOM_FIELD_STORY_POINTS_ID, None)
    if story_points is None or (isinstance(story_points, float) and np.isnan(story_points)):
        return 0
    return story_points

def _calculate_completion_time(histories, created_date, status):
    if status.lower() not in ["done", "qa complete", "released", "closed"]:
        return 0
    
    for history in histories:
        for item in history['items']:
            if item['field'] == 'status' and item['toString'].lower() in ['done', 'qa complete', 'released', 'closed']:
                resolved_date = datetime.strptime(history['created'], "%Y-%m-%dT%H:%M:%S.%f%z")
                return (resolved_date - created_date).days
    return 0

def _process_bug_metrics(issue_type, histories, sprint_start_date, sprint_end_date):
    if issue_type.lower() != "bug":
        return 0, 0, 0
    
    bug_count = 1
    bugs_time_sprint = get_logged_time_per_sprint(histories, sprint_start_date, sprint_end_date)
    bugs_time_all = get_logged_time(histories)
    return bug_count, bugs_time_sprint, bugs_time_all

# === EXTRACT ISSUE META ===
def extract_issue_meta(issue, issue_data, selected_summary_duration_name, team_name, sprint_start_datetime, sprint_end_date, log_list):
    key = issue.get("key", "")
    # _, sprint_start_date, sprint_end_date = show_sprint_name_start_date_and_end_date(selected_summary_duration_name, log_list)

    fields = issue_data['fields']
    if not fields:
        append_log(log_list, "error", f"No fields found for issue {key}.")
        return {}
    
    histories = issue_data['changelog']['histories']
    sprints = fields.get("customfield_10010", [])
    issue_type = fields.get('issuetype', {}).get('name', '')
    status = fields.get('status', {}).get('name', '')
    created_date = datetime.strptime(issue_data['fields']['created'], "%Y-%m-%dT%H:%M:%S.%f%z")

    story_points = _get_story_points(fields)
    sprint_count = len(sprints) if isinstance(sprints, list) else 1
    spillover_issues = 1 if sprint_count > 1 else 0
    spillover_story_points = story_points if sprint_count > 1 else 0
    completion_time_days = _calculate_completion_time(histories, created_date, status)
    sprint_start_date = sprint_start_datetime.date() if hasattr(sprint_start_datetime, 'date') else sprint_start_datetime
    bug_count, bugs_time_sprint, bugs_time_all = _process_bug_metrics(issue_type, histories, sprint_start_date, sprint_end_date)
    issues_closed = 1 if status.lower() in ["done", "qa complete", "released", "closed"] else 0
    failed_qa_count = count_transitions(histories, "In Testing", "Rejected") or 0
    worked_time_sprint = get_logged_time_per_sprint(histories, sprint_start_date, sprint_end_date)
    total_all_time = get_logged_time(histories)
    
    # Track scope changes: issues added/removed during sprint after 1 hour
    added_to_sprint = 0
    removed_from_sprint = 0
    
    if sprint_start_datetime:
        try:
            # Get target sprint name with team prefix
            target_sprint_name = None
            if selected_summary_duration_name == "Current Sprint":
                from common import get_sprint_for_date
                from datetime import date
                sprint_number, _, _ = get_sprint_for_date(date.today().strftime("%Y-%m-%d"))
                target_sprint_name = f"{team_name} {sprint_number}"
            elif selected_summary_duration_name.startswith("Sprint "):
                sprint_number = selected_summary_duration_name.replace("Sprint ", "")
                target_sprint_name = f"{team_name} {sprint_number}"
            
            if target_sprint_name:
                from zoneinfo import ZoneInfo
                issue_was_added = False
                issue_was_removed = False
                
                append_log(log_list, "info", f"Processing scope changes for {key} - target sprint: {target_sprint_name}, sprint start: {sprint_start_datetime.strftime('%Y-%m-%d %H:%M:%S %Z')} America/New_York")
                
                # Special logging for POS-613 and POS-614
                if key in ['POS-613', 'POS-614']:
                    append_log(log_list, "info", f"🎯 DEBUGGING {key}: Starting scope change analysis")
                    append_log(log_list, "info", f"🎯 DEBUGGING {key}: Found {len(histories)} history entries")
                    
                    # Show ALL Sprint field changes regardless of timing
                    all_sprint_changes = []
                    for h in histories:
                        h_date = datetime.strptime(h['created'], "%Y-%m-%dT%H:%M:%S.%f%z").astimezone(ZoneInfo('America/New_York'))
                        for item in h['items']:
                            if item['field'] == 'Sprint':
                                all_sprint_changes.append(f"  {h_date.strftime('%Y-%m-%d %H:%M:%S')} NY: '{item.get('fromString', '')}' → '{item.get('toString', '')}'")
                    
                    if all_sprint_changes:
                        append_log(log_list, "info", f"🎯 DEBUGGING {key}: ALL Sprint field changes in history:")
                        for change in all_sprint_changes:
                            append_log(log_list, "info", f"🎯 DEBUGGING {key}: {change}")
                    else:
                        append_log(log_list, "info", f"🎯 DEBUGGING {key}: NO Sprint field changes found in entire history")
                
                for history in histories:
                    history_date_utc = datetime.strptime(history['created'], "%Y-%m-%dT%H:%M:%S.%f%z")
                    # Convert history date to America/New_York timezone
                    history_date = history_date_utc.astimezone(ZoneInfo('America/New_York'))
                    
                    hours_after_start = (history_date - sprint_start_datetime).total_seconds() / 3600
                    
                    # Special logging for POS-613 and POS-614
                    if key in ['POS-613', 'POS-614']:
                        append_log(log_list, "info", f"🎯 DEBUGGING {key}: History entry at {history_date.strftime('%Y-%m-%d %H:%M:%S')} NY ({hours_after_start:.1f}h after start)")
                        sprint_changes = [item for item in history['items'] if item['field'] == 'Sprint']
                        if sprint_changes:
                            for item in sprint_changes:
                                append_log(log_list, "info", f"🎯 DEBUGGING {key}: Sprint change - From: '{item.get('fromString', '')}' To: '{item.get('toString', '')}'")
                                if hours_after_start <= SCOPE_CHANGE_GRACE_PERIOD_HOURS:
                                    append_log(log_list, "info", f"🎯 DEBUGGING {key}: ⚠️  This change is within {SCOPE_CHANGE_GRACE_PERIOD_HOURS}-hour grace period - will be ignored")
                        else:
                            append_log(log_list, "info", f"🎯 DEBUGGING {key}: No Sprint field changes in this history entry")
                    
                    if hours_after_start <= SCOPE_CHANGE_GRACE_PERIOD_HOURS:
                        continue
                        
                    for item in history['items']:
                        if item['field'] == 'Sprint':
                            from_sprint = item.get('fromString', '') or ''
                            to_sprint = item.get('toString', '') or ''
                            
                            append_log(log_list, "info", f"Issue {key}: Sprint change at {history_date.strftime('%Y-%m-%d %H:%M:%S')} NY ({hours_after_start:.1f}h after start) - From: '{from_sprint}' To: '{to_sprint}'")
                            
                            # Check if this change involves our target sprint
                            target_in_from = target_sprint_name in from_sprint
                            target_in_to = target_sprint_name in to_sprint
                            
                            # Issue added to target sprint
                            if target_in_to and not target_in_from:
                                if not issue_was_added:  # Only count once per issue
                                    issue_was_added = True
                                    if key in ['POS-612', 'POS-613', 'POS-614', 'POS-615']:
                                        append_log(log_list, "info", f"🎯 TRACKING {key}: ADDED to {target_sprint_name} at {history_date.strftime('%Y-%m-%d %H:%M:%S')} America/New_York ({hours_after_start:.1f}h after sprint start)")
                                    append_log(log_list, "info", f"🔍 SCOPE CHANGE - ADDED: {key} to {target_sprint_name}")
                            # Issue removed from target sprint  
                            elif target_in_from and not target_in_to:
                                if not issue_was_removed:  # Only count once per issue
                                    issue_was_removed = True
                                    append_log(log_list, "info", f"🔍 SCOPE CHANGE - REMOVED: {key} from {target_sprint_name}")
                
                # Set final values (each issue can only be counted once)
                added_to_sprint = 1 if issue_was_added else 0
                removed_from_sprint = 1 if issue_was_removed else 0
                
                append_log(log_list, "info", f"Issue {key} final scope change: added={added_to_sprint}, removed={removed_from_sprint}")
                        
        except Exception as e:
            append_log(log_list, "error", f"Error processing scope changes for {key}: {e}")
    


    return {
        "key": key,
        "story_points": story_points, 
        "issues_closed": issues_closed,
        "sprint_hours": worked_time_sprint,   
        "all_time_hours": total_all_time,
        "bug_count": bug_count,
        "failed_qa_count": failed_qa_count,
        "sprint_count": sprint_count,
        "completion_time_days": completion_time_days,
        "spillover_issues": spillover_issues,
        "spillover_story_points": spillover_story_points,
        "bugs_hours_in_current_sprint": bugs_time_sprint,
        "total_all_time_bugs_hours": bugs_time_all,
        "added_to_sprint": added_to_sprint,
        "removed_from_sprint": removed_from_sprint
    }