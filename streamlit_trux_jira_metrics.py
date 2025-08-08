import streamlit as st
from datetime import datetime
from collections import OrderedDict
from datetime import datetime, date
import numpy as np

from common import connection_setup, prepare_detailed_jql_query, get_previous_n_sprints, show_sprint_name_start_date_and_end_date
from report_detailed import generate_detailed_report, generated_report_df_display
from report_summary import generate_summary_report, generated_summary_report_df_display, generated_summary_report_df_display

TEAMS_DATA = OrderedDict([
    ("A Team", "34e068f6-978d-4ad9-a4ef-3bf5eec72f65"),
    ("Avengers", "8d39d512-0220-4711-9ad0-f14fbf74a50e"),
    ("Jarvis", "1ec8443e-a42c-4613-bc88-513ee29203d0"),
    ("Mavrix", "1d8f251a-8fd9-4385-8f5f-6541c28bda19"),
    ("Phoenix", "ac9cc58b-b860-4c4d-8a4e-5a64f50c5122"),
    ("Quantum", "99b45e3f-49de-446c-b28d-25ef8e915ad6")
])

SUMMARY_DURATIONS_DATA = OrderedDict([
    ("Current Sprint", "openSprints()")
])

DETAILED_DURATIONS_DATA = OrderedDict([
    ("Current Sprint", "1"),
    ("Year to Date", "startOfYear()"),
    ("Current Month", "startOfMonth()"),
    ("Last Month", "startOfMonth(-1)"),
    ("Last 2 Months", "startOfMonth(-2)"),
    ("Last 3 Months", "startOfMonth(-3)"),
    ("Last 6 Months", "startOfMonth(-6)"),
    ("Custom Date Range", "customDateRange()")
])

# Set Streamlit page configuration
st.set_page_config(
    page_title="Trux Jira Metrics Dashboard",
    layout="wide",
    initial_sidebar_state="auto",
    page_icon=":bar_chart:",
)

st.title("📊 Jira Metrics")
# st.markdown(""" Generate detailed reports from Jira data. """)

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
        previous_sprints = get_previous_n_sprints()  # You can pass custom count, base date if needed

        for sprint in previous_sprints:
            SUMMARY_DURATIONS_DATA[f"Sprint {sprint}"] = sprint

        summary_duration_names = list(SUMMARY_DURATIONS_DATA.keys())
        current_summary_duration_name_for_selector = st.session_state.selected_summary_duration_name
        current_summary_duration_idx = summary_duration_names.index(current_summary_duration_name_for_selector) if current_summary_duration_name_for_selector in summary_duration_names else 0

        # print(f"current_summary_duration_name_for_selector : {current_summary_duration_name_for_selector}")

        def on_summary_duration_selector_change_callback():
            st.session_state.selected_summary_duration_name = st.session_state.summary_duration_selector_widget_key
            print(f"st.session_state.selected_summary_duration_name : {st.session_state.selected_summary_duration_name}")
            st.session_state.selected_summary_duration_func = SUMMARY_DURATIONS_DATA.get(st.session_state.selected_summary_duration_name)

        st.selectbox(
            "Select Duration",
            options=summary_duration_names,
            index=current_summary_duration_idx,
            key="summary_duration_selector_widget_key",
            on_change=on_summary_duration_selector_change_callback,
            help="Select the time duration for filtering issues."
        )

        generate_summary_button = st.button("Generate Summary Report")

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

        cycle_time_threshold_days = st.number_input("Cycle Time Threshold (days)", min_value=1, value=7, step=1, key="cycle_threshold_days_input")
        lead_time_threshold_days = st.number_input("Lead Time Threshold (days)", min_value=1, value=21, step=1, key="lead_threshold_days_input")
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
            
            st.session_state.selected_detailed_custom_start_date = st.date_input("Start Date", value=start_default, key="start_date_input")
            st.session_state.selected_custom_end_date = st.date_input("End Date", value=end_default, key="end_date_input")

        generate_detailed_button = st.button("Generate Detailed Report")


# --- Helper for capturing Streamlit messages (remains in app.py as UI-level helper) ---
def add_log_message(log_list, level, message):
    """Appends a timestamped log message to the log list and optionally displays immediate feedback."""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    log_list.append(f"[{timestamp}] [{level.upper()}] {message}")
    if level == "error" or level == "critical":
        st.error(f"[{timestamp}] {message}")
    elif level == "warning":
        st.warning(f"[{timestamp}] {message}")


# # --- Main Content Area for Report Options ---
logs_placeholder = st.empty()

if generate_summary_button:
    st.session_state.log_messages = [] 
    start_time = datetime.now()
    add_log_message(st.session_state.log_messages, "info", "Generating summary report...")

    sprint_name, sprint_start_date, sprint_end_date = show_sprint_name_start_date_and_end_date(st.session_state.selected_summary_duration_name, st.session_state.log_messages)

    today_str = date.today().strftime("%Y-%m-%d")
    if st.session_state.selected_summary_duration_name == "Current Sprint":
        st.subheader(f"Leading Indicators - Current Sprint - {sprint_name}")
    else:
        st.subheader(f"Leading Indicators - Previous Sprint - {sprint_name}")

    col1, col2, col3 = st.columns(3)
    with col1:
        st.markdown(f"**Today:** {date.today().strftime('%d-%b-%Y')}")

    with col2:
        st.markdown(f"**Start Date:** {sprint_start_date.strftime('%d-%b-%Y')}")

    with col3:
        st.markdown(f"**End Date:** {sprint_end_date.strftime('%d-%b-%Y')}")

    st.markdown("---")
    all_teams = ("\", \"".join(map(str, list(TEAMS_DATA.values()))))

    jira_conn_details = connection_setup(jira_url, jira_email, jira_api_token, st.session_state.log_messages)
    
    if jira_conn_details is not None:

        dataCol = st.columns(1)[0]
        with dataCol:
            with st.spinner("Fetching issues and generating summary report..."):
                team_metrics = generate_summary_report(list(TEAMS_DATA.values()), jira_conn_details, st.session_state.selected_summary_duration_name, st.session_state.log_messages)

                if team_metrics is not None:
                    df_jira_metrics = generated_summary_report_df_display(team_metrics)

                    # Sort by Teams
                    df_jira_metrics = df_jira_metrics.sort_values(by="Teams").reset_index(drop=True)

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
                    styled_df = (
                        df_jira_metrics.style
                        .apply(style_rows, axis=1)
                        .set_table_styles([
                            {'selector': 'th', 'props': [('font-weight', 'bold')]}
                        ])
                        .format(
                            formatter={"% Completed": "{:.0f}%"},
                            na_rep="",
                            precision=0
                        )
                    )

                    # Render as HTML
                    st.markdown(styled_df.to_html(escape=False), unsafe_allow_html=True)

                else:
                    add_log_message(st.session_state.log_messages, "error", "Failed to generate detailed report.")
            


        chartCol = st.columns(1)[0]
        with chartCol: 

            # Define columns you want to visualize
            # metrics_cols = [
            #     "Issues", "Story Points", "Issues Complete", "% Completed", "Hours Worked",
            #     "All Time", "Bugs", "Issues > 1 Sprint", "Points > 1 Sprint", "Sprint/Story"
            # ]

            # # Ensure these columns exist and drop the 'Total' row
            # plot_df = df_jira_metrics[df_jira_metrics["Teams"] != "Total"]

            # # Filter only the required numeric columns
            # if all(col in plot_df.columns for col in metrics_cols):
            #     st.subheader("📊 Team Metrics Overview")
            #     st.bar_chart(plot_df.set_index("Teams")[metrics_cols])
            # else:
            #     st.warning("Some required columns are missing for the chart.")


            # cols_to_plot = ["Issues", "Issues Complete", "Bugs"]
            # if all(col in df_jira_metrics.columns for col in cols_to_plot):
            #     st.subheader("📊 Multiple Metrics per Team")
            #     chart_data = df_jira_metrics[df_jira_metrics["Teams"] != "Total"].set_index("Teams")[cols_to_plot]
            #     st.bar_chart(chart_data)

                # st.dataframe(df_jira_metrics, use_container_width=True)

                end_time = datetime.now()
                add_log_message(st.session_state.log_messages, "info", f"Success: Data fetching complete! Duration: {end_time - start_time}")
        
    else:
        add_log_message(st.session_state.log_messages, "error", "Failed to set up Jira connection. Please check your credentials.")

if generate_detailed_button:
    st.session_state.log_messages = []
    start_time = datetime.now()

    add_log_message(st.session_state.log_messages, "info", "Generating detailed report...")
    jira_conn_details = connection_setup(jira_url, jira_email, jira_api_token, st.session_state.log_messages)
    
    if jira_conn_details is not None:
        jql_query = prepare_detailed_jql_query(st.session_state.selected_team_id, 
                                                st.session_state.selected_detailed_duration_name, 
                                                st.session_state.selected_detailed_duration_func, 
                                                st.session_state.log_messages, 
                                                st.session_state.selected_detailed_custom_start_date, 
                                                st.session_state.selected_detailed_custom_end_date)
        if jql_query is not None:
            with st.spinner("Fetching issues and generating detailed report..."):
                df = generate_detailed_report(jira_conn_details, jql_query, st.session_state.selected_team_name, st.session_state.log_messages)
        
                if df is not None:
                    st.subheader(f"📋 Generated Detailed Report Preview")
                    generated_report_df_display(df, cycle_threshold_hours, lead_threshold_hours, st.session_state.log_messages)

                    end_time = datetime.now()
                    add_log_message(st.session_state.log_messages, "info", f"Success: Data fetching complete! Duration: {end_time - start_time}")
                
        else:
            add_log_message(st.session_state.log_messages, "error", "Failed to generate detailed report.")
    else:
        add_log_message(st.session_state.log_messages, "error", "Failed to set up Jira connection. Please check your credentials.")

    

# Refresh logs in the top placeholder
with logs_placeholder.expander("View Processing Logs", expanded=False):
    if st.session_state.log_messages:
        for log_msg in st.session_state.log_messages:
            st.code(log_msg, language="text")
    else:
        st.info("No logs generated yet. Click 'Generate Summary Report' or 'Generate Detailed Report' to see activity.")