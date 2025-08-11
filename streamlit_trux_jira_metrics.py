import streamlit as st
from datetime import datetime
from collections import OrderedDict
from datetime import datetime, date
import numpy as np
import pandas as pd

from common import connection_setup, prepare_detailed_jql_query, get_previous_n_sprints, show_sprint_name_start_date_and_end_date, DETAILED_DURATIONS_DATA
from report_detailed import generate_detailed_report, generated_report_df_display
from report_summary import generate_summary_report, generated_summary_report_df_display
from comparison_analysis import generate_team_comparison_data, display_comparison_analysis

# === CACHE CONFIGURATION ===
CACHE_TTL_SECONDS = 30  # Cache duration for all @st.cache_data calls


TEAMS_DATA = OrderedDict([
    ("A-Team", "34e068f6-978d-4ad9-a4ef-3bf5eec72f65"),
    ("Avengers", "8d39d512-0220-4711-9ad0-f14fbf74a50e"),
    ("Jarvis", "1ec8443e-a42c-4613-bc88-513ee29203d0"),
    ("Mavrix", "1d8f251a-8fd9-4385-8f5f-6541c28bda19"),
    ("Phoenix", "ac9cc58b-b860-4c4d-8a4e-5a64f50c5122"),
    ("Quantum", "99b45e3f-49de-446c-b28d-25ef8e915ad6")
])

SUMMARY_DURATIONS_DATA = OrderedDict([
    ("Current Sprint", "openSprints()")
])



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

# --- Main Streamlit App Layout ---
# --- Sidebar for Jira Credentials and General Report Options ---
with st.sidebar:
    # JIRA Connection Details (hardcoded)
    jira_url = "https://truxinc.atlassian.net"
    jira_email = "devops@truxnow.com"  # Replace with your JIRA email
    jira_api_token = "ATATT3xFfGF0jW8QvPl3S5MyCZPa1CJt9WmUbTPn0MOr_O5Eh1aePI6tXkdIxrcJKUKa7z7iHLawm3YvYU_zjrAoSPAQkXWZN5V1YekPnBwmjw6tqu_RtmrkDDtnyocECiCBAKN5T6waGfFgm1tRCYfig-xpuO9GvookawoD57V3TRLxQ0qXMvw=0BBD706D"  # Replace with your JIRA API token
    
    with st.expander("Summary Report", expanded=True):
    # st.header("Summary Report")
        sprint_count = st.number_input("Previous Sprints to Include", min_value=1, max_value=10, value=3, step=1, help="Number of previous sprints to show in duration dropdown")
        previous_sprints = get_previous_n_sprints(sprint_count)
        
        # Get current sprint to avoid duplication
        from common import get_sprint_for_date
        from datetime import date
        current_sprint_name, _, _ = get_sprint_for_date(date.today().strftime("%Y-%m-%d"))
        
        # Add only previous sprints (exclude current sprint)
        for sprint in previous_sprints:
            if sprint != current_sprint_name:
                SUMMARY_DURATIONS_DATA[f"Sprint {sprint}"] = sprint

        summary_duration_names = list(SUMMARY_DURATIONS_DATA.keys())
        current_summary_duration_name_for_selector = st.session_state.selected_summary_duration_name
        current_summary_duration_idx = summary_duration_names.index(current_summary_duration_name_for_selector) if current_summary_duration_name_for_selector in summary_duration_names else 0

        def on_summary_duration_selector_change_callback():
            st.session_state.selected_summary_duration_name = st.session_state.summary_duration_selector_widget_key
            st.session_state.selected_summary_duration_func = SUMMARY_DURATIONS_DATA.get(st.session_state.selected_summary_duration_name)

        st.selectbox(
            "Select Duration",
            options=summary_duration_names,
            index=current_summary_duration_idx,
            key="summary_duration_selector_widget_key",
            on_change=on_summary_duration_selector_change_callback,
            help="Select the time duration for filtering issues."
        )

        col1, col2 = st.columns([2, 1])
        with col1:
            if st.button("Generate Summary Report"):
                generate_summary_button = True
            else:
                generate_summary_button = False
        with col2:
            if st.button("🔄 Refresh", help="Force refresh data (bypass cache)"):
                st.cache_data.clear()
                st.session_state.summary_data = None
                st.session_state.comparison_data = None
                st.session_state.last_summary_selection = None
                generate_summary_button = True
            else:
                pass
        
        # Add comparison toggle
        def on_comparison_toggle_change():
            st.session_state.show_comparison = st.session_state.comparison_toggle_widget_key
        
        st.checkbox(
            "Show Team Comparison Analysis", 
            value=st.session_state.show_comparison,
            key="comparison_toggle_widget_key",
            on_change=on_comparison_toggle_change,
            help="Compare teams across different durations"
        )

    st.markdown("---")

    with st.expander("Detailed Report"):
        st.markdown("Report Thresholds")
        # st.markdown("<span style='color:red'>Report Thresholds</span>", unsafe_allow_html=True)

        team_names_display = list(TEAMS_DATA.keys())
        current_team_name_for_selector = st.session_state.selected_team_name
        current_team_idx = team_names_display.index(current_team_name_for_selector) if current_team_name_for_selector in team_names_display else 0
        
        # Callback to update session state (rerun will happen automatically from on_change)
        def on_team_selector_change_callback():
            st.session_state.selected_team_name = st.session_state.team_selector_widget_key
            st.session_state.selected_team_id = TEAMS_DATA.get(st.session_state.selected_team_name)

        st.selectbox(
            "Select Team",
            options=team_names_display,
            index=current_team_idx,
            key="team_selector_widget_key",
            on_change=on_team_selector_change_callback,
            help="Select the team to filter issues."
        )

        def on_threshold_change():
            pass
            
        cycle_time_threshold_days = st.number_input("Cycle Time Threshold (days)", min_value=1, value=7, step=1, key="cycle_threshold_days_input", on_change=on_threshold_change)
        lead_time_threshold_days = st.number_input("Lead Time Threshold (days)", min_value=1, value=21, step=1, key="lead_threshold_days_input", on_change=on_threshold_change)
        cycle_threshold_hours = cycle_time_threshold_days * 24
        lead_threshold_hours = lead_time_threshold_days * 24

        detailed_duration_names = list(DETAILED_DURATIONS_DATA.keys())
        current_detailed_duration_name_for_selector = st.session_state.selected_detailed_duration_name
        current_detailed_duration_idx = detailed_duration_names.index(current_detailed_duration_name_for_selector) if current_detailed_duration_name_for_selector in detailed_duration_names else 0

        def on_detailed_duration_selector_change_callback():
            st.session_state.selected_detailed_duration_name = st.session_state.detailed_duration_selector_widget_key
            st.session_state.selected_detailed_duration_func = DETAILED_DURATIONS_DATA.get(st.session_state.selected_detailed_duration_name)

        st.selectbox(
            "Select Duration",
            options=detailed_duration_names,
            index=current_detailed_duration_idx,
            key="detailed_duration_selector_widget_key",
            on_change=on_detailed_duration_selector_change_callback,
            help="Select the time duration for filtering issues."
        )

        if st.session_state.selected_detailed_duration_name == "Custom Date Range":
            start_default = st.session_state.selected_detailed_custom_start_date if st.session_state.selected_detailed_custom_start_date else date(2025, 1, 1)
            end_default = st.session_state.selected_detailed_custom_end_date if st.session_state.selected_detailed_custom_end_date else date.today()
            
            def on_date_change():
                pass
            
            st.session_state.selected_custom_start_date = st.date_input("Start Date", value=start_default, key="start_date_input", on_change=on_date_change)
            st.session_state.selected_custom_end_date = st.date_input("End Date", value=end_default, key="end_date_input", on_change=on_date_change)

        col1, col2 = st.columns([2, 1])
        with col1:
            if st.button("Generate Detailed Report"):
                generate_detailed_button = True
            else:
                generate_detailed_button = False
        with col2:
            if st.button("🔄 Refresh", help="Force refresh data (bypass cache)", key="detailed_refresh"):
                st.cache_data.clear()
                st.session_state.detailed_data = None
                st.session_state.detailed_header = None
                st.session_state.last_detailed_selection = None
                generate_detailed_button = True
            else:
                pass





# # --- Main Content Area for Report Options ---
styled_summary_df = None

tab_summary, tab_detailed = st.tabs(["Summary Report", "Detailed Report"])

with tab_summary:



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
            - **Issues Completed**: Issues with status: "Done", "QA Complete", "Released", or "Closed"
            - **% Complete**: Percentage of completed issues out of total issues
            - **Hours Worked**: Time logged during the current sprint period (in hours)
            - **All Time**: Total time logged across all sprints for these issues (in hours)
            - **Bugs**: Number of issues with type "Bug"
            - **Failed QA Count**: Number of times issues transitioned from "In Testing" to "Rejected"
            - **Spillover Issues**: Issues that span multiple sprints
            - **Spillover Story Points**: Story points from issues that span multiple sprints
            - **Avg Completion Days**: Average number of days from issue creation to completion
            - **Avg Sprints/Story**: Average number of sprints per story for completed issues
            """)
        
        # Display comparison analysis if enabled
        if st.session_state.show_comparison and st.session_state.comparison_data:
            st.markdown("---")
            display_comparison_analysis(
                st.session_state.comparison_data, 
                TEAMS_DATA, 
                st.session_state.selected_summary_duration_name
            )
    else:
        st.info("Click 'Generate Summary Report' to view the summary data.")
        
        # Show comparison toggle even when no data
        if st.session_state.show_comparison:
            st.info("Enable comparison analysis by generating a summary report first.")

with tab_detailed:
    if st.session_state.detailed_header is not None:
        st.markdown(st.session_state.detailed_header, unsafe_allow_html=True)
    
    if st.session_state.detailed_data is not None:
        common_message = "This report is filtered and excludes sub-tasks"
        status_message = "This report is filtered and excludes sub-tasks. Includes only issues with status 'QA Complete', 'Released', or 'Closed'."
        
        # Show info message
        if st.session_state.selected_detailed_duration_name == "Current Sprint":
            st.info(f"ℹ️ {common_message}.")
        elif st.session_state.selected_detailed_duration_name == "Custom Date Range":
            st.info(f"ℹ️ {status_message}.")
        else:
            st.info(f"ℹ️ {status_message}.")
        
        from report_detailed import generated_report_df_display
        generated_report_df_display(st.session_state.detailed_data, cycle_threshold_hours, lead_threshold_hours, st.session_state.detailed_log_messages)
    else:
        st.info("Click 'Generate Detailed Report' to view the detailed data.")




if generate_summary_button:
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
    
    header_html = f"""
    <h3>{header_title}</h3>
    <div style="display: flex; justify-content: space-between; margin-bottom: 20px;">
        <div><strong>Today:</strong> {date.today().strftime('%d-%b-%Y')}</div>
        <div><strong>Sprint Start Date:</strong> {sprint_start_date.strftime('%d-%b-%Y')}</div>
        <div><strong>Sprint End Date:</strong> {sprint_end_date.strftime('%d-%b-%Y')}</div>
    </div>
    <hr>
    """
    
    st.session_state.summary_header = header_html
    all_teams = ("\", \"".join(map(str, list(TEAMS_DATA.values()))))

    jira_conn_details = connection_setup(jira_url, jira_email, jira_api_token, st.session_state.summary_log_messages)

    if jira_conn_details is not None:
        with st.spinner("Fetching issues and generating summary report..."):
            # Use caching for summary report with timestamp to ensure fresh calls on button click
            @st.cache_data(ttl=CACHE_TTL_SECONDS)
            def cached_summary_report(teams_list, conn_details, duration_name, teams_data_tuple, timestamp):
                return generate_summary_report(teams_list, conn_details, duration_name, dict(teams_data_tuple), st.session_state.summary_log_messages)
            
            # Convert TEAMS_DATA to tuple for cache key stability and add timestamp
            teams_data_tuple = tuple(TEAMS_DATA.items())
            button_timestamp = datetime.now().timestamp()  # Fresh timestamp on each button click
            team_metrics = cached_summary_report(tuple(TEAMS_DATA.values()), jira_conn_details, st.session_state.selected_summary_duration_name, teams_data_tuple, button_timestamp)
            
            # Generate comparison data if requested (only once)
            if st.session_state.show_comparison and st.session_state.comparison_data is None:
                with st.spinner("Generating comparison data across all durations..."):
                    all_durations = list(SUMMARY_DURATIONS_DATA.keys())
                    
                    @st.cache_data(ttl=CACHE_TTL_SECONDS)
                    def cached_comparison_data(conn_details, teams_tuple, durations_tuple, timestamp):
                        return generate_team_comparison_data(conn_details, dict(teams_tuple), list(durations_tuple), st.session_state.summary_log_messages)
                    
                    st.session_state.comparison_data = cached_comparison_data(
                        jira_conn_details, tuple(TEAMS_DATA.items()), tuple(all_durations), button_timestamp
                    )

            if team_metrics is not None:
                df_jira_metrics = generated_summary_report_df_display(team_metrics, TEAMS_DATA)

                # Separate Grand Total row and sort only team rows
                grand_total_row = df_jira_metrics[df_jira_metrics["Teams"] == "Grand Total"]
                team_rows = df_jira_metrics[df_jira_metrics["Teams"] != "Grand Total"]
                
                # Sort team rows by Teams column
                team_rows_sorted = team_rows.sort_values(by="Teams").reset_index(drop=True)
                
                # Combine sorted team rows with Grand Total at the bottom
                df_jira_metrics = pd.concat([team_rows_sorted, grand_total_row], ignore_index=True)

                df_jira_metrics.index = np.arange(1, len(df_jira_metrics) + 1)

                # Styling function
                def style_rows(row):
                    is_last = row.name == df_jira_metrics.index[-1]
                    is_even = row.name % 2 == 0

                    styles = []
                    for _ in row:
                        if is_last:
                            styles.append('font-weight: bold; background-color: #f4f4f4')
                        elif is_even:
                            styles.append('background-color: #f9f9f9')
                        else:
                            styles.append('')
                    return styles

                # Apply styles
                styled_summary_df = (
                    df_jira_metrics.style
                    .apply(style_rows, axis=1)
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

if generate_detailed_button:
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


# Check if any individual section is fullscreen
if st.session_state.summary_logs_fullscreen:
    # Summary logs fullscreen - display in main area
    st.markdown("**Summary Report Logs - Fullscreen**")
    if st.button("⊞", key="summary_fullscreen_restore", help="Restore to normal view"):
        st.session_state.summary_logs_fullscreen = False
        st.rerun()
    
    if st.session_state.summary_log_messages:
        for log_msg in st.session_state.summary_log_messages:
            st.code(log_msg, language="text")
    else:
        st.info("No summary logs yet.")

elif st.session_state.detailed_logs_fullscreen:
    # Detailed logs fullscreen - display in main area
    st.markdown("**Detailed Report Logs - Fullscreen**")
    if st.button("⊞", key="detailed_fullscreen_restore", help="Restore to normal view"):
        st.session_state.detailed_logs_fullscreen = False
        st.rerun()
    
    if st.session_state.detailed_log_messages:
        for log_msg in st.session_state.detailed_log_messages:
            st.code(log_msg, language="text")
    else:
        st.info("No detailed logs yet.")

else:
    # Normal: show logs in expander with two column layout
    with st.expander("View Processing Logs", expanded=False):
        col1, col2 = st.columns(2)
        
        with col1:
            summary_title = f"**Summary Report Logs - {st.session_state.selected_summary_duration_name}**"
            st.markdown(summary_title)
            
            # Summary logs controls
            if st.button("⛶", key="summary_logs_fullscreen_toggle", help="Expand to fullscreen"):
                st.session_state.summary_logs_fullscreen = True
                st.rerun()
            
            if st.session_state.summary_log_messages:
                for log_msg in st.session_state.summary_log_messages:
                    st.code(log_msg, language="text")
            else:
                st.info("No summary logs yet.")
        
        with col2:
            detailed_title = f"**Detailed Report Logs - {st.session_state.selected_team_name} - {st.session_state.selected_detailed_duration_name}**"
            st.markdown(detailed_title)
            
            # Detailed logs controls
            if st.button("⛶", key="detailed_logs_fullscreen_toggle", help="Expand to fullscreen"):
                st.session_state.detailed_logs_fullscreen = True
                st.rerun()
            
            if st.session_state.detailed_log_messages:
                for log_msg in st.session_state.detailed_log_messages:
                    st.code(log_msg, language="text")
            else:
                st.info("No detailed logs yet.")

# Simple tab switching at the end
if generate_summary_button or generate_detailed_button:
    tab_to_switch = 0 if generate_summary_button else 1
    st.markdown(
        f"""
        <script>
        setTimeout(function() {{
            const tabs = document.querySelectorAll('[data-baseweb="tab"]');
            if (tabs.length > {tab_to_switch}) {{
                tabs[{tab_to_switch}].click();
            }}
        }}, 200);
        </script>
        """,
        unsafe_allow_html=True
    )

# === PROCESSING LOGS SECTION (AT BOTTOM) ===





