import pandas as pd
import streamlit as st
from report_summary import generate_summary_report, generated_summary_report_df_display, SUMMARY_COLUMNS

def generate_team_comparison_data(jira_conn_details, teams_data, all_durations, log_list):
    """Generate comparison data for all teams across all durations"""
    from concurrent.futures import ThreadPoolExecutor, as_completed
    
    comparison_data = {}
    
    def process_duration(duration_name):
        team_metrics = generate_summary_report(
            tuple(teams_data.values()), 
            jira_conn_details, 
            duration_name, 
            teams_data, 
            []
        )
        return duration_name, team_metrics
    
    # Process all durations in parallel
    with ThreadPoolExecutor(max_workers=min(5, len(all_durations))) as executor:
        futures = {executor.submit(process_duration, duration): duration for duration in all_durations}
        
        for future in as_completed(futures):
            duration_name, team_metrics = future.result()
            if team_metrics:
                comparison_data[duration_name] = team_metrics
    
    return comparison_data

def create_team_performance_comparison(comparison_data, teams_data):
    """Create team performance comparison across durations"""
    if not comparison_data:
        return None
    
    # Order durations: Current Sprint first, then previous sprints in descending order
    ordered_durations = []
    if 'Current Sprint' in comparison_data:
        ordered_durations.append('Current Sprint')
    
    sprint_durations = [d for d in comparison_data.keys() if d.startswith('Sprint ')]
    sprint_durations.sort(key=lambda x: tuple(map(int, x.replace('Sprint ', '').split('.'))), reverse=True)
    ordered_durations.extend(sprint_durations)
    
    team_id_to_name = {v: k for k, v in teams_data.items()}
    comparison_rows = []
    
    for team_id in teams_data.values():
        team_name = team_id_to_name[team_id]
        row = [team_name]
        
        for duration_name in ordered_durations:
            if duration_name in comparison_data:
                team_metric = comparison_data[duration_name].get(team_id, {})
                completion_pct = team_metric.get(SUMMARY_COLUMNS['PERCENT_COMPLETED'], 0)
                row.append(f"{completion_pct:.0f}%")
        
        comparison_rows.append(row)
    
    columns = ['Team'] + ordered_durations
    return pd.DataFrame(comparison_rows, columns=columns)

def create_metric_comparison_table(comparison_data, teams_data, metric_key, metric_name):
    """Create comparison table for a specific metric"""
    if not comparison_data:
        return None
    
    # Order durations: Current Sprint first, then previous sprints in descending order
    ordered_durations = []
    if 'Current Sprint' in comparison_data:
        ordered_durations.append('Current Sprint')
    
    sprint_durations = [d for d in comparison_data.keys() if d.startswith('Sprint ')]
    sprint_durations.sort(key=lambda x: tuple(map(int, x.replace('Sprint ', '').split('.'))), reverse=True)
    ordered_durations.extend(sprint_durations)
    
    team_id_to_name = {v: k for k, v in teams_data.items()}
    comparison_rows = []
    
    for team_id in teams_data.values():
        team_name = team_id_to_name[team_id]
        row = [team_name]
        
        for duration_name in ordered_durations:
            if duration_name in comparison_data:
                team_metric = comparison_data[duration_name].get(team_id, {})
                value = team_metric.get(metric_key, 0)
                row.append(value)
        
        comparison_rows.append(row)
    
    columns = ['Team'] + ordered_durations
    return pd.DataFrame(comparison_rows, columns=columns)

def display_comparison_analysis(comparison_data, teams_data, selected_duration):
    """Display comprehensive comparison analysis"""
    if not comparison_data:
        st.warning("No comparison data available")
        return
    
    st.subheader("📊 Team Performance Comparison")
    
    # Team vs Team comparison for selected duration
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("**Team Comparison - Selected Duration**")
        if selected_duration in comparison_data:
            selected_metrics = comparison_data[selected_duration]
            team_id_to_name = {v: k for k, v in teams_data.items()}
            
            comparison_df = []
            for team_id, metrics in selected_metrics.items():
                team_name = team_id_to_name[team_id]
                comparison_df.append({
                    'Team': team_name,
                    'Completion %': f"{metrics.get(SUMMARY_COLUMNS['PERCENT_COMPLETED'], 0):.0f}%",
                    'Story Points': metrics.get(SUMMARY_COLUMNS['STORY_POINTS'], 0),
                    'Sprint Hours': metrics.get(SUMMARY_COLUMNS['SPRINT_HOURS'], 0)
                })
            
            df = pd.DataFrame(comparison_df).sort_values('Team')
            st.dataframe(df, hide_index=True, use_container_width=True)
    
    with col2:
        st.markdown("**Completion % Across Durations**")
        completion_df = create_team_performance_comparison(comparison_data, teams_data)
        if completion_df is not None:
            if selected_duration in completion_df.columns:
                styled_df = completion_df.style.set_properties(subset=[selected_duration], **{'background-color': '#fff2cc'})
                st.dataframe(styled_df, hide_index=True, use_container_width=True)
            else:
                st.dataframe(completion_df, hide_index=True, use_container_width=True)
    
    # Detailed metric comparisons
    st.markdown("**Detailed Metric Comparisons**")
    
    tab1, tab2, tab3 = st.tabs(["Story Points", "Sprint Hours", "Bugs"])
    
    with tab1:
        story_points_df = create_metric_comparison_table(
            comparison_data, teams_data, 
            SUMMARY_COLUMNS['STORY_POINTS'], 'Story Points'
        )
        if story_points_df is not None:
            # Convert story points to integers
            for col in story_points_df.columns[1:]:
                story_points_df[col] = story_points_df[col].round(0).astype(int)
            if selected_duration in story_points_df.columns:
                styled_df = story_points_df.style.set_properties(subset=[selected_duration], **{'background-color': '#fff2cc'})
                st.dataframe(styled_df, hide_index=True, use_container_width=True)
            else:
                st.dataframe(story_points_df, hide_index=True, use_container_width=True)
    
    with tab2:
        sprint_hours_df = create_metric_comparison_table(
            comparison_data, teams_data, 
            SUMMARY_COLUMNS['SPRINT_HOURS'], 'Sprint Hours'
        )
        if sprint_hours_df is not None:
            # Round sprint hours to whole numbers
            for col in sprint_hours_df.columns[1:]:
                sprint_hours_df[col] = sprint_hours_df[col].round(0).astype(int)
            if selected_duration in sprint_hours_df.columns:
                styled_df = sprint_hours_df.style.set_properties(subset=[selected_duration], **{'background-color': '#fff2cc'})
                st.dataframe(styled_df, hide_index=True, use_container_width=True)
            else:
                st.dataframe(sprint_hours_df, hide_index=True, use_container_width=True)
    
    with tab3:
        bugs_df = create_metric_comparison_table(
            comparison_data, teams_data, 
            SUMMARY_COLUMNS['BUGS'], 'Bugs'
        )
        if bugs_df is not None:
            if selected_duration in bugs_df.columns:
                styled_df = bugs_df.style.set_properties(subset=[selected_duration], **{'background-color': '#fff2cc'})
                st.dataframe(styled_df, hide_index=True, use_container_width=True)
            else:
                st.dataframe(bugs_df, hide_index=True, use_container_width=True)