import pandas as pd
import numpy as np
import streamlit as st
from concurrent.futures import ThreadPoolExecutor, as_completed
import requests
from requests.auth import HTTPBasicAuth
from datetime import datetime

from common import seconds_to_hours, get_summary_issues_by_jql, prepare_summary_jql_query, get_issue_changelog, get_logged_time, show_sprint_name_start_date_and_end_date, get_logged_time_per_sprint, get_logged_time_per_sprint, count_transitions


# === GENERATE HEADERS ===
def generate_headers():
    return ["Teams", "Issues", "Story Points", "Issues Complete", "% Complete", "Hours Worked", "All Time", "Bugs", "Failed QA Count", "Issues > 1 Sprint", "Points > 1 Sprint", "Avg Completion Days", "Avg Sprints/Story"]


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
        
        issues = get_summary_issues_by_jql(jql, jira_url, jira_username, jira_api_token, log_list)

        if not issues:
            append_log(log_list, "warn", f"No issues found for team {team_name}. Report will be empty.")
            return team_id, None
        else:
            append_log(log_list, "info", f"Found {len(issues)} issues for team {team_name}.")

        all_metrics = generate_summary_report_streamlit(
            team_name, issues, jira_url, jira_username, jira_api_token, selected_summary_duration_name, log_list
        )

        # do none check
        total_issues = len(all_metrics)
        total_story_points = sum(issue['story_points'] for issue in all_metrics)
        total_issues_closed = sum(issue['issues_closed'] for issue in all_metrics)
        total_hours_worked = sum(issue['hours_worked'] for issue in all_metrics)
        total_all_time = sum(issue['all_time'] for issue in all_metrics)
        total_failed_qa_count = sum(issue['failed_qa_count'] for issue in all_metrics)
        total_bugs = sum(issue['bug_count'] for issue in all_metrics)
        total_spillover_issues = sum(issue['spillover_issues'] for issue in all_metrics)
        total_spillover_points = sum(issue['spillover_story_points'] for issue in all_metrics)

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
            "Total Issues": total_issues,
            "Story Points": total_story_points,
            "Issues Completed": total_issues_closed,
            "% Completed": percent_work_complete,
            "Hours Worked": seconds_to_hours(total_hours_worked),
            "All Time": seconds_to_hours(total_all_time),
            "Bugs": total_bugs,
            "Failed QA Count": total_failed_qa_count,
            "Spillover Issues": total_spillover_issues,
            "Spillover Story Points": total_spillover_points,
            "Avg Completion Days": round(avg_completion_days, 1),
            "Avg Sprints/Story": round(avg_sprints_per_story, 1),
        }

    # Run all teams in parallel
    with ThreadPoolExecutor(max_workers=min(10, len(team_ids))) as executor:
        futures = {executor.submit(process_team, team_id): team_id for team_id in team_ids}

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

def generate_summary_report_streamlit(team_name, issues, jira_url, username, api_token, selected_summary_duration_name, log_list):
    append_log(log_list, "info", f"Collecting metrics for {len(issues)} issues. This may take a while...")
    all_metrics = collect_metrics_streamlit(issues, jira_url, username, api_token, selected_summary_duration_name, log_list)
    
    if not all_metrics:
        append_log(log_list, "warn", "No metrics collected. Report will be empty.")
        return None

    append_log(log_list, "info", f"{team_name} metrics generated successfully!")
    return all_metrics

def collect_metrics_streamlit(issues, jira_url, username, api_token, selected_summary_duration_name, log_list):
    all_metrics = []

    def process_issue(issue):
        try:
            issue_key = issue.get("key", "")
            issue_data = get_issue_changelog(issue_key, jira_url, username, api_token, log_list)
            return extract_issue_meta(issue, issue_data, selected_summary_duration_name, log_list)
        except Exception as e:
            append_log(log_list, "error", f"Error processing issue {issue.get('key', 'unknown')}: {e}")
            return None

    # Process issues in parallel with increased workers
    with ThreadPoolExecutor(max_workers=20) as executor:
        futures = [executor.submit(process_issue, issue) for issue in issues]
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
            metrics.get("Total Issues", 0),
            metrics.get("Story Points", 0),
            metrics.get("Issues Completed", 0),
            metrics.get("% Completed", 0.0),
            metrics.get("Hours Worked", 0.0),
            metrics.get("All Time", 0.0),
            metrics.get("Bugs", 0),
            metrics.get("Failed QA Count", 0),
            metrics.get("Spillover Issues", 0),
            metrics.get("Spillover Story Points", 0),
            metrics.get("Avg Completion Days", 0.0),
            metrics.get("Avg Sprints/Story", 0.0),
        ])

    df = pd.DataFrame(rows, columns=generate_headers())

    # Keep % Complete as numeric for proper alignment

    # add total to each column, for the Teams Column, show label as Total
    total_row = df.select_dtypes(include='number').sum(numeric_only=True)
    
    # Calculate average for % Complete and keep as numeric
    avg_percent = rows and sum(row[4] for row in rows) / len(rows) or 0
    total_row["% Complete"] = avg_percent

    # Add 'Teams' label
    total_row["Teams"] = "Grand Total"

    # Append the row as the last row
    df.loc[len(df)] = total_row

    return df
    

# === EXTRACT ISSUE META ===
def extract_issue_meta(issue, issue_data, selected_summary_duration_name, log_list):
    key = issue.get("key", "")

    _, sprint_start_date, sprint_end_date = show_sprint_name_start_date_and_end_date(selected_summary_duration_name, log_list)

    story_points = 0
    issues_closed = 0
    bug_count = 0
    spillover_issues = 0
    spillover_story_points = 0

    fields = issue_data['fields']
    if not fields:
        append_log(log_list, "error", f"No fields found for issue {key}.")
        return {}
    
    histories = issue_data['changelog']['histories']
    sprints = fields.get("customfield_10010", [])  # Sprint field
    issue_type = fields.get('issuetype', {}).get('name', '')
    status = fields.get('status', {}).get('name', '')

    story_points = fields.get(CUSTOM_FIELD_STORY_POINTS_ID, None) # Default to None if not found
    if story_points is None or (isinstance(story_points, float) and np.isnan(story_points)):
        story_points = 0 # Display "N/A" for missing values

    # Calculate sprint count for this issue
    sprint_count = len(sprints) if isinstance(sprints, list) else 1
    
    if sprint_count > 1:
        spillover_issues += 1
        spillover_story_points += story_points or 0
    
    # Calculate completion time based on created date and resolution date
    created_date = datetime.strptime(issue_data['fields']['created'], "%Y-%m-%dT%H:%M:%S.%f%z")
    completion_time_days = 0
    
    # Check if issue is completed based on current status
    if status.lower() in ["done", "qa complete", "released", "closed"]:
        # Find resolution date from changelog
        for history in histories:
            for item in history['items']:
                if item['field'] == 'status' and item['toString'].lower() in ['done', 'qa complete', 'released', 'closed']:
                    resolved_date = datetime.strptime(history['created'], "%Y-%m-%dT%H:%M:%S.%f%z")
                    completion_time_days = (resolved_date - created_date).days
                    break
            if completion_time_days > 0:
                break

    # Count bugs
    if issue_type.lower() == "bug":
        bug_count += 1

    # Count completed issues
    if status.lower() in ["done", "qa complete", "released", "closed"]:
        issues_closed += 1

    # failed QA count
    failed_qa_count = count_transitions(histories, "In Testing", "Rejected")
    if failed_qa_count is None:
        failed_qa_count = 0  # Default to 0 if no transitions found

    total_all_time_logged_time_in_seconds = get_logged_time(histories)

    worked_in_current_sprint = get_logged_time_per_sprint(histories, sprint_start_date, sprint_end_date)    

    # append_log(log_list, "info", f"Extracted issue meta for {key}: story_points={story_points}, issues_closed={issues_closed}, bug_count={bug_count}, Status={fields['status']['name']}, issues_more_than_1_sprint={issues_more_than_1_sprint}, story_points_more_than_1_sprint={story_points_more_than_1_sprint}")
    return {
        "key": key,
        "story_points": story_points, 
        "issues_closed": issues_closed,
        "hours_worked": worked_in_current_sprint,   
        "all_time": total_all_time_logged_time_in_seconds,
        "bug_count": bug_count,
        "failed_qa_count": failed_qa_count,
        "sprint_count": sprint_count,
        "completion_time_days": completion_time_days,
        "spillover_issues": spillover_issues,
        "spillover_story_points": spillover_story_points,
    }