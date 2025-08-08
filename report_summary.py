import pandas as pd
import numpy as np
import streamlit as st
from concurrent.futures import ThreadPoolExecutor, as_completed
import requests
from requests.auth import HTTPBasicAuth
from datetime import datetime
from collections import defaultdict

from common import get_summary_issues_by_jql, seconds_to_hours, prepare_summary_jql_query, get_issue_changelog, get_logged_time


# === GENERATE HEADERS ===
def generate_headers():
    return ["Teams", "Issues", "Story Points", "Issues Complete", "% Complete", "Hours Worked", "All Time", "Bugs", "Issues > 1 Sprint", "Points > 1 Sprint", "Sprint/Story"]


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

# def generate_summary_report(team_ids, jira_conn_details, selected_summary_duration_name, log_list):
#     jira_url, jira_username, jira_api_token = jira_conn_details

#     team_metrics = {}

#     for team_id in team_ids:
#         def get_team_name_by_id(team_id, teams_data):
#             return next((name for name, tid in teams_data.items() if tid == team_id), None)

#         team_name = get_team_name_by_id(team_id, TEAMS_DATA)

#         jql = prepare_summary_jql_query(team_id, team_name, selected_summary_duration_name, log_list)
#         # jql = f'"Team[Team]" = "{team_id}" AND sprint in openSprints() AND issuetype NOT IN (Sub-task) ORDER BY KEY'
#         # append_log(log_list, "info", f"JQL for team {team_id}: {jql}")
#         issues = get_summary_issues_by_jql(jql, jira_url, jira_username, jira_api_token, log_list)

#         if not issues:
#             append_log(log_list, "warn", "No issues found matching the JQL query. Report will be empty.")
#             st.session_state.generated_summary_report_df_display = None
#             st.warning("No issues found matching the JQL query. Report will be empty.")
#         else:
#             append_log(log_list, "info", f"Found {len(issues)} issues matching the JQL query.")

#         all_metrics = generate_summary_report_streamlit(issues, 
#             jira_url, jira_username, jira_api_token,
#             st.session_state.log_messages
#         )

#         # Calculate metrics
#         total_issues = len(all_metrics)
#         total_story_points = sum(issue['story_points'] for issue in all_metrics)
#         total_issues_closed = sum(issue['issues_closed'] for issue in all_metrics)
#         total_hours_closed = sum(issue['hours_worked'] for issue in all_metrics)
#         total_bugs = sum(issue['bug_count'] for issue in all_metrics)
#         # total_sprint_counts = [issue['sprint_counts'] for issue in all_metrics]
#         total_spillover_issues = sum(issue['spillover_issues'] for issue in all_metrics)
#         total_spillover_points = sum(issue['spillover_story_points'] for issue in all_metrics)

#         percent_work_complete = round((total_issues_closed / total_issues) * 100, 2) if total_issues else 0
#         # avg_sprints_per_issue = round(sum(sprint_counts) / len(sprint_counts), 2) if sprint_counts else 0

#         team_metrics[team_id] = {
#             "Total Issues": total_issues,
#             "Story Points": total_story_points,
#             "Issues Completed": total_issues_closed,
#             "% Completed": percent_work_complete,
#             "Hours Worked": total_hours_closed,
#             "Bugs": total_bugs,
#             "Spillover Issues": total_spillover_issues,
#             "Spillover Story Points": total_spillover_points,
#             # "Sprints/Story Ratio": avg_sprints_per_issue
#         }
        
#     append_log(log_list, "info", "Report generated!")

#     return team_metrics

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
            team_name, issues, jira_url, jira_username, jira_api_token, log_list
        )

        total_issues = len(all_metrics)
        total_story_points = sum(issue['story_points'] for issue in all_metrics)
        total_issues_closed = sum(issue['issues_closed'] for issue in all_metrics)
        total_hours_closed = sum(issue['hours_worked'] for issue in all_metrics)
        total_bugs = sum(issue['bug_count'] for issue in all_metrics)
        total_spillover_issues = sum(issue['spillover_issues'] for issue in all_metrics)
        total_spillover_points = sum(issue['spillover_story_points'] for issue in all_metrics)

        percent_work_complete = round((total_issues_closed / total_issues) * 100, 2) if total_issues else 0
        # calculate avg_sprints_per_issue


        return team_id, {
            "Total Issues": total_issues,
            "Story Points": total_story_points,
            "Issues Completed": total_issues_closed,
            "% Completed": percent_work_complete,
            "Hours Worked": total_hours_closed,
            "Bugs": total_bugs,
            "Spillover Issues": total_spillover_issues,
            "Spillover Story Points": total_spillover_points,
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

def generate_summary_report_streamlit(team_name, issues, jira_url, username, api_token, log_list):
    append_log(log_list, "info", f"Collecting metrics for {len(issues)} issues. This may take a while...")
    all_metrics = collect_metrics_streamlit(issues, jira_url, username, api_token,  log_list)
    
    if not all_metrics:
        append_log(log_list, "warn", "No metrics collected. Report will be empty.")
        return None

    append_log(log_list, "info", f"{team_name} metrics generated successfully!")
    return all_metrics

def collect_metrics_streamlit(issues, jira_url, username, api_token, log_list):
    all_metrics = []

    for issue in issues:
        try:
            issue_key = issue.get("key", "")
            issue_data = get_issue_changelog(issue_key, jira_url, username, api_token, log_list)
            issue_meta = extract_issue_meta(issue, issue_data, log_list)
            all_metrics.append((issue_meta))
        except requests.exceptions.RequestException as req_e:
            append_log(log_list, "error", f"Network error fetching issue {issue}: {req_e}")
        except Exception as e:
            append_log(log_list, "error", f"Error processing issue {issue}: {e}")  

    return all_metrics

# def process_team_issues(team_ids, jira_url, username, api_token, log_list):
#     team_metrics = {}

#     for team_id in team_ids:
#         jql = f'"Team[Team]" = "{team_id}" AND sprint in openSprints() AND issuetype NOT IN (Sub-task)'
#         append_log(log_list, "info", f"JQL for team {team_id}: {jql}")
#         issue_keys = get_issues_by_jql(jql, jira_url, username, api_token, log_list)

#         total_issues = len(issue_keys)
#         if total_issues == 0:
#             append_log(log_list, "warn", f"No issues found for team {team_id}. Skipping.")
#             continue
#         append_log(log_list, "info", f"Processing {total_issues} issues for team {team_id}")

        

#         for issue in issues:
#             # if(issue[key]== "TM-505"):
#             #     print(f"issue: {issue}")
#             fields = issue.get("fields", {})
#             issue_type = fields.get("issuetype", {}).get("name", "")
#             status = fields.get("status", {}).get("name", "")

#             story_points_value = fields.get(CUSTOM_FIELD_STORY_POINTS_ID, None) # Default to None if not found
#             if story_points_value is None or (isinstance(story_points_value, float) and np.isnan(story_points_value)):
#                 story_points_value = 0 # Display "N/A" for missing values
#             elif isinstance(story_points_value, (int, float)):
#                 story_points_value = int(story_points_value) # Cast to int
#             elif isinstance(story_points_value, str) and story_points_value.replace('.','',1).isdigit():
#                 try:
#                     story_points_value = int(float(story_points_value)) # Handle '1.0' as string
#                 except ValueError:
#                     story_points_value = 0 # Fallback if string is not convertible to number
#             else:
#                 story_points_value = str(story_points_value) # Keep as string if some other non-numeric type


#             # sp = fields.get(CUSTOM_FIELD_STORY_POINTS_ID, 0)  # Adjust customfield_10016 if different for story points
#             time_spent = fields.get("timespent", 0) or 0
#             sprints = fields.get("customfield_10010", [])  # Sprint field
#             print(f"issue: {issue['key']}, sprints: {sprints}, story_points_value: {story_points_value}, issue_type.lower(): {issue_type.lower()}, status: {status}")

#             # Count story points
#             story_points += int(story_points_value) or 0

#             # Count bugs
#             if issue_type.lower() == "bug":
#                 bugs += 1

#             # Count completed issues
#             if status.lower() in ["done", "qa complete", "released", "closed"]:
#                 issues_closed += 1

#             # Count hours worked
#             hours_worked += time_spent

#             # Count spillovers
#             if isinstance(sprints, list) and len(sprints) > 1:
#                 spillover_issues += 1
#                 spillover_story_points += sp or 0
#                 sprint_counts.append(len(sprints))

#         # Avoid division by zero
#         percent_complete = round((issues_closed / total_issues) * 100, 2) if total_issues else 0
#         avg_sprints_per_issue = round(sum(sprint_counts) / len(sprint_counts), 2) if sprint_counts else 0

#         team_metrics[team_id] = {
#             "Total Issues": total_issues,
#             "Story Points": story_points,
#             "Issues Completed": issues_closed,
#             "% Completed": percent_complete,
#             "Hours Worked": round(hours_worked / 3600, 2),  # convert seconds to hours
#             "Bugs": bugs,
#             "Spillover Issues": spillover_issues,
#             "Spillover Story Points": spillover_story_points,
#             "Sprints/Story Ratio": avg_sprints_per_issue
#         }

#         append_log(log_list, "info", f"{team_metrics}")
   
#     return team_metrics

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
            "",  # Placeholder for All Time
            metrics.get("Bugs", 0),
            metrics.get("Spillover Issues", 0),
            metrics.get("Spillover Story Points", 0),
            metrics.get("Sprints/Story Ratio", 0),
        ])

    df = pd.DataFrame(rows, columns=generate_headers())

    # add total to each column, for the Teams Column, show label as Total
    total_row = df.select_dtypes(include='number').sum(numeric_only=True)

    # Add 'Teams' label with bold markdown
    total_row["Teams"] = "Total"

    # Append the row as the last row
    df.loc[len(df)] = total_row

    return df
    

# === EXTRACT ISSUE META ===
def extract_issue_meta(issue, issue_data, log_list):
    key = issue.get("key", "")
    story_points = 0
    issues_closed = 0
    bug_count = 0
    sprint_counts = []
    spillover_issues = 0
    spillover_story_points = 0

    fields = issue_data['fields']
    if not fields:
        append_log(log_list, "error", f"No fields found for issue {key}.")
        return {}
    
    histories = issue_data['changelog']['histories']
    # sprints_field = fields.get('customfield_10010')
    sprints = fields.get("customfield_10010", [])  # Sprint field
    issue_type = fields.get('issuetype', {}).get('name', '')
    status = fields.get('status', {}).get('name', '')

    # sprint_str = "N/A"
    # if isinstance(sprints_field, list):        
    #     sprint_names = []
    #     sprints_field.sort(key=lambda x: x.get('id', 0), reverse=True)
    #     for sprint in sprints_field:
    #         if isinstance(sprint, dict) and 'name' in sprint:
    #             sprint_names.append(sprint['name'])
    #     sprint_str = ", ".join(sprint_names) if sprint_names else "N/A"

    story_points = fields.get(CUSTOM_FIELD_STORY_POINTS_ID, None) # Default to None if not found
    if story_points is None or (isinstance(story_points, float) and np.isnan(story_points)):
        story_points = 0 # Display "N/A" for missing values

    if isinstance(sprints, list) and len(sprints) > 1:
        spillover_issues += 1
        spillover_story_points += story_points or 0
        sprint_counts.append(len(sprints))

    # sprint_counts.append(len(sprints))
        # if len(sprint_names) > 1:
        #     issues_more_than_1_sprint += 1
        #     story_points_more_than_1_sprint += story_points

    # Count bugs
    if issue_type.lower() == "bug":
        bug_count += 1

    # Count completed issues
    if status.lower() in ["done", "qa complete", "released", "closed"]:
        issues_closed += 1

    logged_time_in_seconds = get_logged_time(histories)      

    # append_log(log_list, "info", f"Extracted issue meta for {key}: story_points={story_points}, issues_closed={issues_closed}, bug_count={bug_count}, Status={fields['status']['name']}, issues_more_than_1_sprint={issues_more_than_1_sprint}, story_points_more_than_1_sprint={story_points_more_than_1_sprint}")
    return {
        "key": key,
        "story_points": story_points, 
        "issues_closed": issues_closed,
        "hours_worked": seconds_to_hours(logged_time_in_seconds),     
        "bug_count": bug_count,
        "sprint_counts": sprint_counts,
        "spillover_issues": spillover_issues,
        "spillover_story_points": spillover_story_points,
    }