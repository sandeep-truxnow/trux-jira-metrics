import streamlit as st
from datetime import datetime
from collections import OrderedDict
from datetime import datetime, date
import numpy as np
import pandas as pd
import os

from common import connection_setup, prepare_detailed_jql_query, get_previous_n_sprints, show_sprint_name_start_date_and_end_date, DETAILED_DURATIONS_DATA, get_sprint_for_date
from report_detailed import generate_detailed_report, generated_report_df_display
from report_summary import generate_summary_report, generated_summary_report_df_display
from comparison_analysis import generate_team_comparison_data, display_comparison_analysis

# === CACHE CONFIGURATION ===
CACHE_TTL_SECONDS = 30  # Cache duration for all @st.cache_data calls

from config import TEAMS_DATA, SUMMARY_DURATIONS_DATA

# Theme-aware CSS for both light and dark modes
st.markdown(
    """
    <style>
    /* Dark theme styles */
    @media (prefers-color-scheme: dark) {
        .stApp > div:first-child {
            background-color: #0e1117 !important;
        }
        .main .block-container {
            background-color: #0e1117 !important;
            color: #fafafa !important;
        }
        .stMarkdown, .stText {
            color: #fafafa !important;
        }
        div[data-testid="stDataFrame"] {
            background-color: #262730 !important;
        }
        div[data-testid="stDataFrame"] table {
            background-color: #262730 !important;
            color: #fafafa !important;
        }
    }
    
    /* Light theme styles */
    @media (prefers-color-scheme: light) {
        .stApp > div:first-child {
            background-color: #ffffff !important;
        }
        .main .block-container {
            background-color: #ffffff !important;
            color: #262730 !important;
        }
        div[data-testid="stDataFrame"] table {
            background-color: #ffffff !important;
            color: #262730 !important;
        }
    }
    </style>
    """,
    unsafe_allow_html=True
)

# Set Streamlit page configuration
st.set_page_config(
    page_title="Trux Jira Metrics Dashboard",
    layout="wide",
    initial_sidebar_state="auto",
    page_icon=":bar_chart:",
)

# Reduce top space
st.markdown("""
<style>
.main .block-container {
    padding-top: 1rem;
    padding-bottom: 0rem;
}
</style>
""", unsafe_allow_html=True)

st.title("📊 Jira Metrics")

# --- Helper for capturing Streamlit messages ---
def add_log_message(log_list, level, message):
    """Appends a timestamped log message to the log list and optionally displays immediate feedback."""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    log_list.append(f"[{timestamp}] [{level.upper()}] {message}")
    if level == "error" or level == "critical":
        st.error(f"[{timestamp}] {message}")
    elif level == "warning":
        st.warning(f"[{timestamp}] {message}")

# --- Initialize ALL Streamlit session state variables at the TOP LEVEL ---
if 'user_authenticated' not in st.session_state: st.session_state.user_authenticated = False
if 'summary_log_messages' not in st.session_state: st.session_state.summary_log_messages = []
if 'detailed_log_messages' not in st.session_state: st.session_state.detailed_log_messages = []

if 'team_options_display' not in st.session_state: st.session_state.team_options_display = list(TEAMS_DATA.keys())
if 'selected_team_name' not in st.session_state: st.session_state.selected_team_name = list(TEAMS_DATA.keys())[0] 
if 'selected_team_id' not in st.session_state: st.session_state.selected_team_id = TEAMS_DATA[list(TEAMS_DATA.keys())[0]]

if 'summary_duration_options_display' not in st.session_state: st.session_state.summary_duration_options_display = list(SUMMARY_DURATIONS_DATA.keys())
if 'selected_summary_duration_name' not in st.session_state: st.session_state.selected_summary_duration_name = list(SUMMARY_DURATIONS_DATA.keys())[0] 
if 'selected_summary_duration_func' not in st.session_state: st.session_state.selected_summary_duration_func = SUMMARY_DURATIONS_DATA[list(SUMMARY_DURATIONS_DATA.keys())[0]]

if 'detailed_duration_options_display' not in st.session_state: st.session_state.detailed_duration_options_display = list(DETAILED_DURATIONS_DATA.keys())
if 'selected_detailed_duration_name' not in st.session_state: st.session_state.selected_detailed_duration_name = list(DETAILED_DURATIONS_DATA.keys())[0] 
if 'selected_detailed_duration_func' not in st.session_state: st.session_state.selected_detailed_duration_func = DETAILED_DURATIONS_DATA[list(DETAILED_DURATIONS_DATA.keys())[0]]

if 'selected_detailed_custom_start_date' not in st.session_state: st.session_state.selected_detailed_custom_start_date = None
if 'selected_detailed_custom_end_date' not in st.session_state: st.session_state.selected_detailed_custom_end_date = None
if 'active_tab' not in st.session_state: st.session_state.active_tab = 0
if 'summary_data' not in st.session_state: st.session_state.summary_data = None
if 'detailed_data' not in st.session_state: st.session_state.detailed_data = None
if 'summary_header' not in st.session_state: st.session_state.summary_header = None
if 'detailed_header' not in st.session_state: st.session_state.detailed_header = None
if 'last_summary_selection' not in st.session_state: st.session_state.last_summary_selection = None
if 'last_detailed_selection' not in st.session_state: st.session_state.last_detailed_selection = None
if 'switch_to_tab' not in st.session_state: st.session_state.switch_to_tab = None
if 'auto_generate_summary' not in st.session_state: st.session_state.auto_generate_summary = False
if 'auto_generate_detailed' not in st.session_state: st.session_state.auto_generate_detailed = False
if 'comparison_data' not in st.session_state: st.session_state.comparison_data = None
if 'show_comparison' not in st.session_state: st.session_state.show_comparison = False
if 'summary_logs_fullscreen' not in st.session_state: st.session_state.summary_logs_fullscreen = False
if 'detailed_logs_fullscreen' not in st.session_state: st.session_state.detailed_logs_fullscreen = False
if 'scope_change_data' not in st.session_state: st.session_state.scope_change_data = None
if 'scope_time_range' not in st.session_state: st.session_state.scope_time_range = 48

# Initialize button states
if 'generate_summary' not in st.session_state: st.session_state.generate_summary = False
if 'generate_detailed' not in st.session_state: st.session_state.generate_detailed = False

# --- Main Streamlit App Layout ---
# --- Sidebar for Jira Credentials and General Report Options ---
with st.sidebar:
    # Always show authentication section first
    if not st.session_state.user_authenticated:
        with st.expander("User Authentication", expanded=True):
            user_email = st.text_input("User Email", help="Enter your email to get access.", key="user_email_input")
            if st.button("Authenticate", key="auth_btn"):
                try:
                    script_dir = os.path.dirname(os.path.abspath(__file__))
                    file_path = os.path.join(script_dir, "user_auth.txt")

                    with open(file_path, "r", encoding="utf-8") as file:
                        for line in file:
                            user, auth = line.strip().split('|')
                            if user_email.strip().lower() == user.strip().lower() and auth.strip().lower() == "grant":
                                st.session_state.user_authenticated = True
                                st.success("Authenticated successfully.")
                                st.rerun()
                                break
                        else:
                            st.session_state.user_authenticated = False
                            st.error("Access denied.")
                except FileNotFoundError:
                    st.error("Authentication file not found. Contact administrator.")
    
    if st.session_state.user_authenticated:
        # st.markdown("---")
        # JIRA Connection Details (hardcoded)
        jira_url = "https://truxinc.atlassian.net"
        jira_email = "jira-user@truxnow.com"  # Replace with your JIRA email
        jira_api_token = "NWG-ftz4wnc9wum8wkx"  # Replace with your JIRA API token
    
    #### Summary Report Side bar - START ####
    if st.session_state.user_authenticated:
        with st.expander("Summary Report", expanded=True):
            if 'summary_sprint_count' not in st.session_state:
                st.session_state.summary_sprint_count = 3
            summary_sprint_count = st.slider("Previous Sprints to Include", min_value=1, max_value=10, value=st.session_state.summary_sprint_count, help="Number of previous sprints to show in duration dropdown", key="summary_sprint_slider")
            st.session_state.summary_sprint_count = summary_sprint_count
            previous_sprints = get_previous_n_sprints(summary_sprint_count)
            
            # Get current sprint to avoid duplication
            from common import get_sprint_for_date
            from datetime import date
            current_sprint_name, _, _ = get_sprint_for_date(date.today().strftime("%Y-%m-%d"))
            
            # Create a copy and add only previous sprints (exclude current sprint)
            summary_durations_with_sprints = SUMMARY_DURATIONS_DATA.copy()
            for sprint in previous_sprints:
                if sprint != current_sprint_name:
                    summary_durations_with_sprints[f"Sprint {sprint}"] = sprint

            summary_duration_names = list(summary_durations_with_sprints.keys())
            current_summary_duration_name_for_selector = st.session_state.selected_summary_duration_name
            current_summary_duration_idx = summary_duration_names.index(current_summary_duration_name_for_selector) if current_summary_duration_name_for_selector in summary_duration_names else 0

            def on_summary_duration_selector_change_callback():
                st.session_state.selected_summary_duration_name = st.session_state.summary_duration_selector
                st.session_state.selected_summary_duration_func = summary_durations_with_sprints.get(st.session_state.selected_summary_duration_name)

            st.selectbox(
                "Select Duration",
                options=summary_duration_names,
                index=current_summary_duration_idx,
                key="summary_duration_selector",
                on_change=on_summary_duration_selector_change_callback,
                help="Select the time duration for filtering issues."
            )

            col1, col2 = st.columns([2, 1])
            with col1:
                if st.button("Generate Summary Report", key="summary_generate_btn"):
                    st.session_state.generate_summary = True
            with col2:
                if st.button("🔄 Refresh", help="Force refresh data (bypass cache)", key="summary_refresh_btn"):
                    st.cache_data.clear()
                    st.session_state.summary_data = None
                    st.session_state.comparison_data = None
                    st.session_state.last_summary_selection = None
                    st.session_state.generate_summary = True
                    
                    # Generate comparison data if enabled
                    if st.session_state.show_comparison:
                        st.session_state.comparison_data = "loading"
            
            # Add comparison toggle
            def on_comparison_toggle_change():
                st.session_state.show_comparison = st.session_state.comparison_toggle
            
            st.checkbox(
                "Show Team Comparison Analysis", 
                value=st.session_state.show_comparison,
                key="comparison_toggle",
                on_change=on_comparison_toggle_change,
                help="Compare teams across different durations"
            )
            
            scope_time_range = st.slider(
                "Scope Change Time Range (hours)",
                min_value=0,
                max_value=168,
                value=48,
                step=24,
                key="scope_time_slider",
                help="Show scope changes within this time range after sprint start"
            )

    #### Summary Report Side bar - END ####


    #### Detailed Report Side bar - START ####
    if st.session_state.user_authenticated:
        # st.markdown("---")
        with st.expander("Detailed Report"):
            with st.expander("Report Thresholds", expanded=False):
                def on_threshold_change():
                    pass
                    
                cycle_time_threshold_days = st.slider("Cycle Time Threshold (days)", min_value=1, max_value=30, value=7, step=1, key="cycle_threshold_slider", on_change=on_threshold_change)
                lead_time_threshold_days = st.slider("Lead Time Threshold (days)", min_value=1, max_value=60, value=21, step=1, key="lead_threshold_slider", on_change=on_threshold_change)
                
            cycle_threshold_hours = cycle_time_threshold_days * 24
            lead_threshold_hours = lead_time_threshold_days * 24

            team_names_display = list(TEAMS_DATA.keys())
            current_team_name_for_selector = st.session_state.selected_team_name
            current_team_idx = team_names_display.index(current_team_name_for_selector) if current_team_name_for_selector in team_names_display else 0
            
            # Callback to update session state (rerun will happen automatically from on_change)
            def on_team_selector_change_callback():
                st.session_state.selected_team_name = st.session_state.team_selector
                st.session_state.selected_team_id = TEAMS_DATA.get(st.session_state.selected_team_name)

            st.selectbox(
                "Select Team",
                options=team_names_display,
                index=current_team_idx,
                key="team_selector",
                on_change=on_team_selector_change_callback,
                help="Select the team to filter issues."
            )

            # Add previous sprints to detailed durations with separate slider
            if 'detailed_sprint_count' not in st.session_state:
                st.session_state.detailed_sprint_count = 3
            detailed_sprint_count = st.slider("Previous Sprints for Detailed", min_value=1, max_value=10, value=st.session_state.detailed_sprint_count, help="Number of previous sprints for detailed report", key="detailed_sprint_slider")
            st.session_state.detailed_sprint_count = detailed_sprint_count
            previous_sprints_detailed = get_previous_n_sprints(detailed_sprint_count)
            current_sprint_name_detailed, _, _ = get_sprint_for_date(date.today().strftime("%Y-%m-%d"))
            
            # Create a copy of DETAILED_DURATIONS_DATA and add previous sprints
            detailed_durations_with_sprints = DETAILED_DURATIONS_DATA.copy()
            
            # Insert previous sprints after Current Sprint but before Year to Date
            items = list(detailed_durations_with_sprints.items())
            current_sprint_item = items[0]  # Current Sprint
            year_to_date_item = items[1]   # Year to Date
            
            # Rebuild with sprints in between
            detailed_durations_with_sprints = OrderedDict([current_sprint_item])
            for sprint in previous_sprints_detailed:
                if sprint != current_sprint_name_detailed:
                    detailed_durations_with_sprints[f"Sprint {sprint}"] = sprint
            detailed_durations_with_sprints[year_to_date_item[0]] = year_to_date_item[1]

            detailed_duration_names = list(detailed_durations_with_sprints.keys())
            current_detailed_duration_name_for_selector = st.session_state.selected_detailed_duration_name
            current_detailed_duration_idx = detailed_duration_names.index(current_detailed_duration_name_for_selector) if current_detailed_duration_name_for_selector in detailed_duration_names else 0

            def on_detailed_duration_selector_change_callback():
                st.session_state.selected_detailed_duration_name = st.session_state.detailed_duration_selector
                st.session_state.selected_detailed_duration_func = detailed_durations_with_sprints.get(st.session_state.selected_detailed_duration_name)

            st.selectbox(
                "Select Duration",
                options=detailed_duration_names,
                index=current_detailed_duration_idx,
                key="detailed_duration_selector",
                on_change=on_detailed_duration_selector_change_callback,
                help="Select the time duration for filtering issues."
            )

            if st.session_state.selected_detailed_duration_name == "Custom Date Range":
                start_default = st.session_state.selected_detailed_custom_start_date if st.session_state.selected_detailed_custom_start_date else date(2025, 1, 1)
                end_default = st.session_state.selected_detailed_custom_end_date if st.session_state.selected_detailed_custom_end_date else date.today()
                
                def on_date_change():
                    pass
                
                st.session_state.selected_custom_start_date = st.date_input("Start Date", value=start_default, key="start_date_picker", on_change=on_date_change)
                st.session_state.selected_custom_end_date = st.date_input("End Date", value=end_default, key="end_date_picker", on_change=on_date_change)

            col1, col2 = st.columns([2, 1])
            with col1:
                if st.button("Generate Detailed Report", key="detailed_generate_btn"):
                    st.session_state.generate_detailed = True
            with col2:
                if st.button("🔄 Refresh", help="Force refresh data (bypass cache)", key="detailed_refresh_btn"):
                    st.cache_data.clear()
                    st.session_state.detailed_data = None
                    st.session_state.detailed_header = None
                    st.session_state.last_detailed_selection = None
                    st.session_state.generate_detailed = True

    #### Detailed Report Side bar - END ####

# Define variables for authenticated users
if st.session_state.user_authenticated:
    # These variables are defined in the sidebar when authenticated
    pass
else:
    # Default values when not authenticated
    jira_url = None
    jira_email = None
    jira_api_token = None
    cycle_threshold_hours = 168
    lead_threshold_hours = 504

# --- Main Content Area for Report Options ---
if st.session_state.user_authenticated:
    tab_summary, tab_detailed = st.tabs(["Summary Report", "Detailed Report"])

    #### Summary Tab - START    ####
    with tab_summary:
        with st.expander("📊 Summary Report", expanded=True):
            if st.session_state.summary_header is not None:
                st.markdown(st.session_state.summary_header, unsafe_allow_html=True)
            
            if st.session_state.summary_data is not None:
                st.info("ℹ️ This report is filtered and excludes sub-tasks.")
                st.dataframe(
                    st.session_state.summary_data, 
                    use_container_width=True, 
                    hide_index=True,
                    column_config={
                        "Teams": st.column_config.TextColumn(
                            "Teams",
                            pinned="left"
                        ),
                        "Completion %": st.column_config.NumberColumn(
                            "Completion %",
                            format="%.0f%%"
                        )
                    }
                )
            
                # Add collapsible legend below the summary table
                with st.expander("📋 Column Definitions", expanded=False):
                    st.markdown("""
                    - **Teams**: Team name
                    - **Total Issues**: Total number of issues assigned to the team
                    - **Story Points**: Sum of story points for all issues
                    - **Issues Completed**: Issues with status: "Done", "QA Complete", "In UAT", "Ready for Release", "Released", or "Closed"
                    - **Story Points Burnt**: Story points completed with percentage of total (e.g., "5 (20%)")
                    - **% Complete**: Percentage of completed issues out of total issues
                    - **Hours Worked**: Time logged during the current sprint period (in hours)
                    - **All Time**: Total time logged across all sprints for these issues (in hours)
                    - **Bugs**: Number of issues with type "Bug"
                    - **Failed QA Count**: Number of times issues transitioned from "In Testing" to "Rejected"
                    - **Spillover Issues**: Issues that span multiple sprints
                    - **Spillover Story Points**: Story points from issues that span multiple sprints
                    - **Avg Completion Days**: Average number of days from issue creation to completion
                    - **Avg Sprints/Story**: Average number of sprints per story for completed issues
                    - **Scope Changes (Issues & SPs)**: Issues added (+) or removed (-) from sprint with story points in format: +<issues> / -<issues> (+<story points> / -<story points>) within selected time range after sprint start
                    
                    """)
            else:
                st.info("Click 'Generate Summary Report' to view the summary data.")
        
        # Display comparison analysis if enabled
        if st.session_state.show_comparison:
            with st.expander("📈 Team Comparison Analysis", expanded=True):
                if st.session_state.comparison_data == "loading":
                    st.info("📊 Team comparison analysis will be available after clicking refresh")
                elif st.session_state.comparison_data and isinstance(st.session_state.comparison_data, dict):
                    display_comparison_analysis(
                        st.session_state.comparison_data, 
                        TEAMS_DATA, 
                        st.session_state.selected_summary_duration_name
                    )


    #### Detailed Tab - START    ####
    with tab_detailed:
        if st.session_state.detailed_header is not None:
            st.markdown(st.session_state.detailed_header, unsafe_allow_html=True)
        
        if st.session_state.detailed_data is not None:
            common_message = "This report is filtered and excludes sub-tasks"
            status_message = "This report is filtered and excludes sub-tasks. Includes only issues with status 'Done', 'QA Complete', 'In UAT', 'Ready for Release', 'Released', 'Closed'"
            
            # Show info messages
            if st.session_state.selected_detailed_duration_name == "Current Sprint":
                st.info(f"ℹ️ {common_message}.")
            elif st.session_state.selected_detailed_duration_name == "Custom Date Range":
                st.info(f"ℹ️ {status_message}.")
            else:
                st.info(f"ℹ️ {status_message}.")
            
            st.info(f"ℹ️ **Report Thresholds:** Cycle Time > {cycle_time_threshold_days} days and Lead Time > {lead_time_threshold_days} days are highlighted as exceeding thresholds.")
            
            from report_detailed import generated_report_df_display
            generated_report_df_display(st.session_state.detailed_data, cycle_threshold_hours, lead_threshold_hours, st.session_state.detailed_log_messages)
        else:
            st.info("Click 'Generate Detailed Report' to view the detailed data.")



    #### ------------- Report generation logic  ------------- ####
    if st.session_state.generate_summary:
        st.session_state.generate_summary = False
        current_selection = st.session_state.selected_summary_duration_name
        st.session_state.summary_log_messages = [] 
        start_time = datetime.now()
        add_log_message(st.session_state.summary_log_messages, "info", "Generating summary report...")
        st.session_state.last_summary_selection = current_selection

        sprint_name, sprint_start_date, sprint_end_date = show_sprint_name_start_date_and_end_date(st.session_state.selected_summary_duration_name, st.session_state.summary_log_messages)

        # Create header HTML to store in session state
        if st.session_state.selected_summary_duration_name == "Current Sprint":
            header_title = f"Leading Indicators - Current Sprint - {sprint_name}"
        else:
            header_title = f"Leading Indicators - Previous Sprint - {sprint_name}"

        # Calculate time elapsed percentage
        if st.session_state.selected_summary_duration_name == "Current Sprint":
            total_sprint_days = (sprint_end_date - sprint_start_date).days
            elapsed_days = (date.today() - sprint_start_date).days
            time_elapsed_percent = round((elapsed_days / total_sprint_days) * 100) if total_sprint_days > 0 else 0
        else:
            time_elapsed_percent = 100
        
        # Build header HTML conditionally
        days_remaining_html = ""
        if st.session_state.selected_summary_duration_name == "Current Sprint":
            days_diff = np.busday_count(date.today(), sprint_end_date)
            days_remaining_html = f'<div><strong>{days_diff} Days left</strong> </div>'
        
        header_html = f"""
        <h3>{header_title}</h3>
        <div style="display: flex; justify-content: space-between; margin-bottom: 20px;">
            <div><strong>Today:</strong> {date.today().strftime('%d-%b-%Y')}</div>
            <div><strong>Sprint Start Date:</strong> {sprint_start_date.strftime('%d-%b-%Y')} | Sprint End Date:</strong> {sprint_end_date.strftime('%d-%b-%Y')}</div>
            <div><strong><strong>Time Elapsed:</strong> {time_elapsed_percent}% | <strong>Work Complete:</strong> 0%</div>
            {days_remaining_html}
        </div>
        <hr>
        """
        
        st.session_state.summary_header = header_html
        all_teams = ("\", \"".join(map(str, list(TEAMS_DATA.values()))))

        jira_conn_details = connection_setup(jira_url, jira_email, jira_api_token, st.session_state.summary_log_messages)

        if jira_conn_details is not None:
            print(f"Jira connection details: {jira_conn_details}")
            add_log_message(st.session_state.summary_log_messages, "info", f"Jira connection details: {jira_conn_details}")
            add_log_message(st.session_state.summary_log_messages, "info", "jira_conn_details successfully set up")
            with st.spinner("Fetching issues and generating summary report..."):
                # Use caching for summary report with timestamp to ensure fresh calls on button click
                @st.cache_data(ttl=CACHE_TTL_SECONDS)
                def cached_summary_report(teams_list, conn_details, duration_name, teams_data_tuple, timestamp, scope_hours):
                    return generate_summary_report(teams_list, conn_details, duration_name, dict(teams_data_tuple), st.session_state.summary_log_messages, scope_hours)
                
                # Convert TEAMS_DATA to tuple for cache key stability and add timestamp
                teams_data_tuple = tuple(TEAMS_DATA.items()) if TEAMS_DATA else ()
                button_timestamp = datetime.now().timestamp()  # Fresh timestamp on each button click
                teams_tuple = tuple(TEAMS_DATA.values()) if TEAMS_DATA else ()
                
                if not teams_tuple:
                    add_log_message(st.session_state.summary_log_messages, "error", "No teams data available")
                else:
                    team_metrics = cached_summary_report(teams_tuple, jira_conn_details, st.session_state.selected_summary_duration_name, teams_data_tuple, button_timestamp, st.session_state.scope_time_range)
                
                # Generate comparison data if enabled and not already available
                if st.session_state.show_comparison and st.session_state.comparison_data is None:
                    # Use the same durations as the summary dropdown (includes previous sprints)
                    all_durations = list(summary_durations_with_sprints.keys())
                    try:
                        st.session_state.comparison_data = generate_team_comparison_data(
                            jira_conn_details, TEAMS_DATA, all_durations, st.session_state.summary_log_messages, st.session_state.scope_time_range
                        )
                        add_log_message(st.session_state.summary_log_messages, "info", "Comparison data generated successfully")
                    except Exception as e:
                        add_log_message(st.session_state.summary_log_messages, "error", f"Failed to generate comparison data: {e}")
                        st.session_state.comparison_data = None

                if team_metrics is not None:
                    # Calculate work complete percentage from grand total
                    df_jira_metrics = generated_summary_report_df_display(team_metrics, TEAMS_DATA)
                    grand_total_row = df_jira_metrics[df_jira_metrics["Teams"] == "Grand Total"]
                    if not grand_total_row.empty:
                        issues_completed_str = grand_total_row["Issues Completed"].iloc[0]
                        if " (" in issues_completed_str and "%" in issues_completed_str:
                            work_complete_percent = int(issues_completed_str.split("(")[1].split("%")[0])
                            # Update header with actual work complete percentage
                            header_html = header_html.replace("Work Complete:</strong> 0%", f"Work Complete:</strong> {work_complete_percent}%")
                            st.session_state.summary_header = header_html

                    # Separate Grand Total row and sort only team rows
                    grand_total_row = df_jira_metrics[df_jira_metrics["Teams"] == "Grand Total"]
                    team_rows = df_jira_metrics[df_jira_metrics["Teams"] != "Grand Total"]
                    
                    # Sort team rows by Teams column
                    team_rows_sorted = team_rows.sort_values(by="Teams").reset_index(drop=True)
                    
                    # Combine sorted team rows with Grand Total at the bottom
                    df_jira_metrics = pd.concat([team_rows_sorted, grand_total_row], ignore_index=True)

                    df_jira_metrics.index = np.arange(1, len(df_jira_metrics) + 1)

                    # Apply styles with theme-aware colors
                    styled_summary_df = (
                        df_jira_metrics.style
                        .apply(lambda row: [
                            'font-weight: bold; background-color: rgba(40, 167, 69, 0.3); border-top: 3px solid rgba(40, 167, 69, 0.8);' if row.name == df_jira_metrics.index[-1]
                            # else 'background-color: rgba(128, 128, 128, 0.1)' if row.name % 2 == 0
                            else '' for _ in row
                        ], axis=1)
                        .set_table_styles([
                            {'selector': 'th', 'props': [('font-weight', 'bold')]},
                            {'selector': 'table', 'props': [('width', '100%'), ('table-layout', 'fixed')]},
                            {'selector': 'th, td', 'props': [('width', '9.09%'), ('text-align', 'center'), ('padding', '8px')]},
                            {'selector': '.row_heading', 'props': [('display', 'none')]},
                            {'selector': '.blank', 'props': [('display', 'none')]}
                        ])
                        .format(
                            formatter={"% Completed": "{:.0f}%"},
                            na_rep="",
                            precision=0
                        )
                    )

                    # Store summary data in session state
                    st.session_state.summary_data = styled_summary_df
                    end_time = datetime.now()
                    processing_time = end_time - start_time
                    add_log_message(st.session_state.summary_log_messages, "info", f"Summary data stored with {len(df_jira_metrics)} rows")
                    add_log_message(st.session_state.summary_log_messages, "info", f"Total processing time: {processing_time}")
                    st.rerun()

                else:
                    add_log_message(st.session_state.summary_log_messages, "error", "Failed to generate summary report.")
        else:
            add_log_message(st.session_state.summary_log_messages, "error", "Failed to set up Jira connection. Please check your credentials.")



    ### -------------   Generate Detailed Report -------------  ###
    if st.session_state.generate_detailed:
        st.session_state.generate_detailed = False
        add_log_message(st.session_state.detailed_log_messages, "info", "Generate detailed button clicked, switching to detailed tab")
        
        current_detailed_selection = (st.session_state.selected_team_id, st.session_state.selected_detailed_duration_name)
        st.session_state.detailed_log_messages = []
        start_time = datetime.now()
        st.session_state.last_detailed_selection = current_detailed_selection

        # Create detailed header HTML
        if st.session_state.selected_detailed_duration_name == "Current Sprint":
            header_title = f"Detailed Report - {st.session_state.selected_team_name} - Current Sprint"
        elif st.session_state.selected_detailed_duration_name == "Custom Date Range":
            header_title = f"Detailed Report - {st.session_state.selected_team_name} - {st.session_state.selected_custom_start_date} to {st.session_state.selected_custom_end_date}"
        else:
            header_title = f"Detailed Report - {st.session_state.selected_team_name} - {st.session_state.selected_detailed_duration_name}"
        
        detailed_header_html = f"""
        <h3>{header_title}</h3>
        <div style="display: flex; justify-content: space-between; margin-bottom: 20px;">
            <div><strong>Today:</strong> {date.today().strftime('%d-%b-%Y')}</div>
            <div><strong>Team:</strong> {st.session_state.selected_team_name}</div>
            <div><strong>Duration:</strong> {st.session_state.selected_detailed_duration_name}</div>
        </div>
        <hr>
        """
        
        st.session_state.detailed_header = detailed_header_html
        add_log_message(st.session_state.detailed_log_messages, "info", "Generating detailed report...")
        jira_conn_details = connection_setup(jira_url, jira_email, jira_api_token, st.session_state.detailed_log_messages)

        if jira_conn_details is not None:
            jql_query = prepare_detailed_jql_query(st.session_state.selected_team_id, 
                                                    st.session_state.selected_detailed_duration_name, 
                                                    st.session_state.detailed_log_messages)
            
            add_log_message(st.session_state.detailed_log_messages, "info", f"Detailed JQL Query: {jql_query}")
            
            with st.spinner("Fetching issues and generating detailed report..."):
                # Use caching for detailed report with timestamp to ensure fresh calls on button click
                @st.cache_data(ttl=CACHE_TTL_SECONDS)
                def cached_detailed_report(conn_details, query, team_name, timestamp):
                    return generate_detailed_report(conn_details, query, team_name, st.session_state.detailed_log_messages)
                
                button_timestamp = datetime.now().timestamp()  # Fresh timestamp on each button click
                detailed_report_df = cached_detailed_report(jira_conn_details, jql_query, st.session_state.selected_team_name, button_timestamp)
                
                if detailed_report_df is not None:
                    st.session_state.detailed_data = detailed_report_df
                    end_time = datetime.now()
                    processing_time = end_time - start_time
                    add_log_message(st.session_state.detailed_log_messages, "info", "Detailed report generated successfully!")
                    add_log_message(st.session_state.detailed_log_messages, "info", f"Total processing time: {processing_time}")
                    st.rerun()
                else:
                    add_log_message(st.session_state.detailed_log_messages, "error", "Failed to generate detailed report.")
        else:
            add_log_message(st.session_state.detailed_log_messages, "error", "Failed to set up Jira connection. Please check your credentials.")

else:
    st.info("Please authenticate to access the dashboard.")