import streamlit as st
from datetime import datetime
from collections import OrderedDict
from datetime import datetime, date
import numpy as np
import pandas as pd

from common import connection_setup, prepare_detailed_jql_query, get_previous_n_sprints, show_sprint_name_start_date_and_end_date, DETAILED_DURATIONS_DATA
from report_detailed import generate_detailed_report, generated_report_df_display
from report_summary import generate_summary_report, generated_summary_report_df_display

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
if 'log_messages' not in st.session_state: st.session_state.log_messages = []
if 'jira_conn_details' not in st.session_state: st.session_state.jira_conn_details = None

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
if 'last_summary_selection' not in st.session_state: st.session_state.last_summary_selection = None
if 'last_detailed_selection' not in st.session_state: st.session_state.last_detailed_selection = None
if 'switch_to_tab' not in st.session_state: st.session_state.switch_to_tab = None
if 'auto_generate_summary' not in st.session_state: st.session_state.auto_generate_summary = False
if 'auto_generate_detailed' not in st.session_state: st.session_state.auto_generate_detailed = False

# --- Main Streamlit App Layout ---
# --- Sidebar for Jira Credentials and General Report Options ---
with st.sidebar:
    st.header("JIRA Connection Details")
    jira_url = "https://truxinc.atlassian.net"
    jira_email = st.text_input("Jira Email", value=st.session_state.jira_conn_details[1] if st.session_state.jira_conn_details else "", help="Your Jira email or username for API access.", key="sidebar_jira_username")
    jira_api_token = st.text_input("Jira API Token", type="password", value=st.session_state.jira_conn_details[2] if st.session_state.jira_conn_details else "", help="Generate from your Atlassian account security settings.", key="sidebar_jira_api_token")
    
    st.markdown("---")
    
    with st.expander("Summary Report", expanded=True):
    # st.header("Summary Report")
        previous_sprints = get_previous_n_sprints(3)
        
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
            # print(f"st.session_state.selected_summary_duration_name : {st.session_state.selected_summary_duration_name}")
            st.session_state.selected_summary_duration_func = SUMMARY_DURATIONS_DATA.get(st.session_state.selected_summary_duration_name)
            # Clear data when selection changes
            st.session_state.summary_data = None
            st.session_state.summary_header = None
            st.session_state.last_summary_selection = None

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
                st.session_state.last_summary_selection = None
                generate_summary_button = True
            else:
                pass

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
            # Clear only detailed data when team selection changes
            st.session_state.detailed_data = None
            st.session_state.last_detailed_selection = None
            # Switch to detailed tab
            st.session_state.switch_to_tab = 1
            add_log_message(st.session_state.log_messages, "info", f"Team changed to: {st.session_state.selected_team_name}, switching to tab 1")

        st.selectbox(
            "Select Team",
            options=team_names_display,
            index=current_team_idx,
            key="team_selector_widget_key",
            on_change=on_team_selector_change_callback,
            help="Select the team to filter issues."
        )

        def on_threshold_change():
            st.session_state.switch_to_tab = 1
            
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
            # Clear data when selection changes
            st.session_state.detailed_data = None
            st.session_state.last_detailed_selection = None
            # Switch to detailed tab
            st.session_state.switch_to_tab = 1

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
                st.session_state.detailed_data = None
                st.session_state.last_detailed_selection = None
                st.session_state.switch_to_tab = 1
            
            st.session_state.selected_custom_start_date = st.date_input("Start Date", value=start_default, key="start_date_input", on_change=on_date_change)
            st.session_state.selected_custom_end_date = st.date_input("End Date", value=end_default, key="end_date_input", on_change=on_date_change)

        if st.button("Generate Detailed Report"):
            generate_detailed_button = True
            # add_log_message(st.session_state.log_messages, "info", "Detailed report button clicked")
        else:
            generate_detailed_button = False





# # --- Main Content Area for Report Options ---
logs_placeholder = st.empty()
styled_summary_df = None

tab_summary, tab_detailed = st.tabs(["Summary Report", "Detailed Report"])

with tab_summary:



    if st.session_state.summary_header is not None:
        st.markdown(st.session_state.summary_header, unsafe_allow_html=True)
    
    if st.session_state.summary_data is not None:
        st.dataframe(
            st.session_state.summary_data, 
            use_container_width=True, 
            hide_index=True,
            column_config={
                "% Complete": st.column_config.NumberColumn(
                    "% Complete",
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
    else:
        st.info("Click 'Generate Summary Report' to view the summary data.")

with tab_detailed:
    common_message = "This report is filtered and excludes sub-tasks"
    status_message = "This report is filtered and excludes sub-tasks. Includes only issues with status 'QA Complete', 'Released', or 'Closed'."

    if st.session_state.detailed_data is not None:
        # Show human-readable description
        if st.session_state.selected_detailed_duration_name == "Current Sprint":
            description = f"Showing all issues assigned to **{st.session_state.selected_team_name}** team in the current active sprint. __{common_message}__."
        elif st.session_state.selected_detailed_duration_name == "Custom Date Range":
            description = f"Showing all issues assigned to **{st.session_state.selected_team_name}** team from {st.session_state.selected_custom_start_date} to {st.session_state.selected_custom_end_date}. __{status_message}__."
        else:
            description = f"Showing all issues assigned to **{st.session_state.selected_team_name}** team for {st.session_state.selected_detailed_duration_name}. __{status_message}__."
        
        st.markdown(description)
        # st.markdown("---")
        
        from report_detailed import generated_report_df_display
        generated_report_df_display(st.session_state.detailed_data, cycle_threshold_hours, lead_threshold_hours, st.session_state.log_messages)
    else:
        st.info("Click 'Generate Detailed Report' to view the detailed data.")




if generate_summary_button:
    
    current_selection = st.session_state.selected_summary_duration_name
    
    # Check if selection changed or no data exists
    if (st.session_state.last_summary_selection == current_selection and 
        st.session_state.summary_data is not None):
        add_log_message(st.session_state.log_messages, "info", "Using cached summary data - no selection change detected.")
    else:
        st.session_state.log_messages = [] 
        start_time = datetime.now()
        add_log_message(st.session_state.log_messages, "info", "Generating summary report...")
        st.session_state.last_summary_selection = current_selection

        sprint_name, sprint_start_date, sprint_end_date = show_sprint_name_start_date_and_end_date(st.session_state.selected_summary_duration_name, st.session_state.log_messages)

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

        jira_conn_details = connection_setup(jira_url, jira_email, jira_api_token, st.session_state.log_messages)
    
        if jira_conn_details is not None:
            with st.spinner("Fetching issues and generating summary report..."):
                # Use caching for summary report
                @st.cache_data(ttl=300)  # Cache for 5 minutes
                def cached_summary_report(teams_list, conn_details, duration_name):
                    return generate_summary_report(teams_list, conn_details, duration_name, TEAMS_DATA, st.session_state.log_messages)
                
                team_metrics = cached_summary_report(tuple(TEAMS_DATA.values()), jira_conn_details, st.session_state.selected_summary_duration_name)

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
                    add_log_message(st.session_state.log_messages, "info", f"Summary data stored with {len(df_jira_metrics)} rows")
                    st.rerun()

                else:
                    add_log_message(st.session_state.log_messages, "error", "Failed to generate detailed report.")
                    
                    end_time = datetime.now()
                    add_log_message(st.session_state.log_messages, "info", f"Success: Data fetching complete! Duration: {end_time - start_time}")
        else:
            add_log_message(st.session_state.log_messages, "error", "Failed to set up Jira connection. Please check your credentials.")

if generate_detailed_button:
    add_log_message(st.session_state.log_messages, "info", "Generate detailed button clicked, switching to detailed tab")
    
    current_detailed_selection = (st.session_state.selected_team_id, st.session_state.selected_detailed_duration_name)
    
    # Check if selection changed or no data exists
    if (st.session_state.last_detailed_selection == current_detailed_selection and 
        st.session_state.detailed_data is not None):
        add_log_message(st.session_state.log_messages, "info", "Using cached detailed data - no selection change detected.")
    else:
        st.session_state.log_messages = []
        start_time = datetime.now()
        st.session_state.last_detailed_selection = current_detailed_selection

        add_log_message(st.session_state.log_messages, "info", "Generating detailed report...")
        jira_conn_details = connection_setup(jira_url, jira_email, jira_api_token, st.session_state.log_messages)
    
        if jira_conn_details is not None:
            jql_query = prepare_detailed_jql_query(st.session_state.selected_team_id, 
                                                    st.session_state.selected_detailed_duration_name, 
                                                    st.session_state.log_messages)
            
            add_log_message(st.session_state.log_messages, "info", f"Detailed JQL Query: {jql_query}")
            
            with st.spinner("Fetching issues and generating detailed report..."):
                detailed_report_df = generate_detailed_report(jira_conn_details, jql_query, st.session_state.selected_team_name, st.session_state.log_messages)
                
                if detailed_report_df is not None:
                    st.session_state.detailed_data = detailed_report_df
                    add_log_message(st.session_state.log_messages, "info", "Detailed report generated successfully!")
                    st.rerun()
                else:
                    add_log_message(st.session_state.log_messages, "error", "Failed to generate detailed report.")
                    
                end_time = datetime.now()
                add_log_message(st.session_state.log_messages, "info", f"Success: Data fetching complete! Duration: {end_time - start_time}")
        else:
            add_log_message(st.session_state.log_messages, "error", "Failed to set up Jira connection. Please check your credentials.")


# Refresh logs in the top placeholder
with logs_placeholder.expander("View Processing Logs", expanded=False):
    if st.session_state.log_messages:
        for log_msg in st.session_state.log_messages:
            st.code(log_msg, language="text")
    else:
        st.info("No logs generated yet. Click 'Generate Summary Report' or 'Generate Detailed Report' to see activity.")

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





