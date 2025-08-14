import pandas as pd
import streamlit as st
from report_summary import generate_summary_report, generated_summary_report_df_display, SUMMARY_COLUMNS

def generate_team_comparison_data(jira_conn_details, teams_data, all_durations, log_list, scope_hours=72):
    """Generate comparison data for all teams across all durations"""
    from concurrent.futures import ThreadPoolExecutor, as_completed
    
    comparison_data = {}
    
    def process_duration(duration_name):
        team_metrics = generate_summary_report(
            tuple(teams_data.values()), 
            jira_conn_details, 
            duration_name, 
            teams_data, 
            log_list,
            scope_hours
        )
        return duration_name, team_metrics
    
    # Process all durations in parallel
    import streamlit as st
    from streamlit.runtime.scriptrunner import add_script_run_ctx
    
    with ThreadPoolExecutor(max_workers=min(5, len(all_durations))) as executor:
        futures = {}
        for duration in all_durations:
            future = executor.submit(process_duration, duration)
            add_script_run_ctx(future)
            futures[future] = duration
        
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
                # Extract percentage from Issues Completed format "X (Y%)"
                issues_completed_str = team_metric.get(SUMMARY_COLUMNS['ISSUES_COMPLETED'], "0 (0%)")
                if " (" in issues_completed_str and "%" in issues_completed_str:
                    completion_pct = int(issues_completed_str.split("(")[1].split("%")[0])
                else:
                    completion_pct = 0
                row.append(f"{completion_pct}%")
        
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
                # Extract sprint hours from combined format "X / Y"
                hours_combined = metrics.get(SUMMARY_COLUMNS['HOURS_COMBINED'], "0 / 0")
                sprint_hours = int(float(hours_combined.split(" / ")[0])) if " / " in hours_combined else 0
                
                issues_completed_str = metrics.get(SUMMARY_COLUMNS['ISSUES_COMPLETED'], "0 (0%)")
                completion_pct = "0%"
                if " (" in issues_completed_str and "%" in issues_completed_str:
                    completion_pct = issues_completed_str.split("(")[1].split(")")[0]
                
                comparison_df.append({
                    'Team': team_name,
                    'Completion %': completion_pct,
                    'Story Points': int(round(metrics.get(SUMMARY_COLUMNS['STORY_POINTS'], 0))),
                    'Sprint Hours': sprint_hours
                })
            
            df = pd.DataFrame(comparison_df).sort_values('Team')
            styled_df = df.style.set_properties(subset=['Completion %'], **{'text-align': 'right'})
            st.dataframe(styled_df, hide_index=True, use_container_width=True)
    
    with col2:
        st.markdown("**Completion % Across Durations**")
        completion_df = create_team_performance_comparison(comparison_data, teams_data)
        if completion_df is not None:
            if selected_duration in completion_df.columns:
                styled_df = completion_df.style.set_properties(subset=[selected_duration], **{'background-color': 'rgba(173, 216, 230, 0.4)'})
                styled_df = styled_df.set_properties(subset=[col for col in completion_df.columns if '%' in str(completion_df[col].iloc[0]) if len(completion_df) > 0], **{'text-align': 'right'})
                st.dataframe(styled_df, hide_index=True, use_container_width=True)
            else:
                styled_df = completion_df.style.set_properties(subset=[col for col in completion_df.columns if '%' in str(completion_df[col].iloc[0]) if len(completion_df) > 0], **{'text-align': 'right'})
                st.dataframe(styled_df, hide_index=True, use_container_width=True)
    
    # Detailed metric comparisons
    st.markdown("**Detailed Metric Comparisons**")
    
    tab1, tab2, tab3, tab4, tab5 = st.tabs(["Issues", "Story Points", "Bugs", "Sprint Hours", "Scope Changes"])
    
    with tab1:
        # Create custom issues comparison with completed/total format
        if comparison_data:
            ordered_durations = []
            if 'Current Sprint' in comparison_data:
                ordered_durations.append('Current Sprint')
            sprint_durations = [d for d in comparison_data.keys() if d.startswith('Sprint ')]
            sprint_durations.sort(key=lambda x: tuple(map(int, x.replace('Sprint ', '').split('.'))), reverse=True)
            ordered_durations.extend(sprint_durations)
            
            team_id_to_name = {v: k for k, v in teams_data.items()}
            issues_rows = []
            
            for team_id in teams_data.values():
                team_name = team_id_to_name[team_id]
                row = [team_name]
                
                for duration_name in ordered_durations:
                    if duration_name in comparison_data:
                        team_metric = comparison_data[duration_name].get(team_id, {})
                        total_issues = team_metric.get(SUMMARY_COLUMNS['TOTAL_ISSUES'], 0)
                        issues_completed_str = team_metric.get(SUMMARY_COLUMNS['ISSUES_COMPLETED'], "0 (0%)")
                        completed_issues = int(issues_completed_str.split(" (")[0]) if " (" in issues_completed_str else 0
                        completion_pct = int(issues_completed_str.split("(")[1].split("%")[0]) if " (" in issues_completed_str and "%" in issues_completed_str else 0
                        row.append(f"{completed_issues}/{total_issues} ({completion_pct}%)")
                
                issues_rows.append(row)
            
            columns = ['Team'] + ordered_durations
            issues_df = pd.DataFrame(issues_rows, columns=columns)
            
            if selected_duration in issues_df.columns:
                styled_df = issues_df.style.set_properties(subset=[selected_duration], **{'background-color': 'rgba(173, 216, 230, 0.4)'})
                st.dataframe(styled_df, hide_index=True, use_container_width=True)
            else:
                st.dataframe(issues_df, hide_index=True, use_container_width=True)
    
    with tab2:
        # Create custom story points comparison with burnt/total format
        if comparison_data:
            team_id_to_name = {v: k for k, v in teams_data.items()}
            story_points_rows = []
            
            for team_id in teams_data.values():
                team_name = team_id_to_name[team_id]
                row = [team_name]
                
                for duration_name in ordered_durations:
                    if duration_name in comparison_data:
                        team_metric = comparison_data[duration_name].get(team_id, {})
                        total_story_points = int(round(team_metric.get(SUMMARY_COLUMNS['STORY_POINTS'], 0)))
                        story_points_burnt_str = team_metric.get(SUMMARY_COLUMNS['STORY_POINTS_BURNT'], "0 (0%)")
                        burnt_points = int(float(story_points_burnt_str.split(" (")[0])) if " (" in story_points_burnt_str else 0
                        burnt_pct = int(story_points_burnt_str.split("(")[1].split("%")[0]) if " (" in story_points_burnt_str and "%" in story_points_burnt_str else 0
                        row.append(f"{burnt_points}/{total_story_points} ({burnt_pct}%)")
                
                story_points_rows.append(row)
            
            columns = ['Team'] + ordered_durations
            story_points_df = pd.DataFrame(story_points_rows, columns=columns)
            
            if selected_duration in story_points_df.columns:
                styled_df = story_points_df.style.set_properties(subset=[selected_duration], **{'background-color': 'rgba(173, 216, 230, 0.4)'})
                st.dataframe(styled_df, hide_index=True, use_container_width=True)
            else:
                st.dataframe(story_points_df, hide_index=True, use_container_width=True)
    
    with tab3:
        bugs_df = create_metric_comparison_table(
            comparison_data, teams_data, 
            SUMMARY_COLUMNS['BUGS'], 'Bugs'
        )
        if bugs_df is not None:
            if selected_duration in bugs_df.columns:
                styled_df = bugs_df.style.set_properties(subset=[selected_duration], **{'background-color': 'rgba(173, 216, 230, 0.4)'})
                st.dataframe(styled_df, hide_index=True, use_container_width=True)
            else:
                st.dataframe(bugs_df, hide_index=True, use_container_width=True)
    
    with tab4:
        # Create custom sprint hours comparison extracting from combined format
        if comparison_data:
            team_id_to_name = {v: k for k, v in teams_data.items()}
            sprint_hours_rows = []
            
            for team_id in teams_data.values():
                team_name = team_id_to_name[team_id]
                row = [team_name]
                
                for duration_name in ordered_durations:
                    if duration_name in comparison_data:
                        team_metric = comparison_data[duration_name].get(team_id, {})
                        hours_combined = team_metric.get(SUMMARY_COLUMNS['HOURS_COMBINED'], "0 / 0")
                        sprint_hours = int(float(hours_combined.split(" / ")[0])) if " / " in hours_combined else 0
                        row.append(sprint_hours)
                
                sprint_hours_rows.append(row)
            
            columns = ['Team'] + ordered_durations
            sprint_hours_df = pd.DataFrame(sprint_hours_rows, columns=columns)
            
            if selected_duration in sprint_hours_df.columns:
                styled_df = sprint_hours_df.style.set_properties(subset=[selected_duration], **{'background-color': 'rgba(173, 216, 230, 0.4)'})
                st.dataframe(styled_df, hide_index=True, use_container_width=True)
            else:
                st.dataframe(sprint_hours_df, hide_index=True, use_container_width=True)
    
    with tab5:
        scope_changes_df = create_metric_comparison_table(
            comparison_data, teams_data, 
            SUMMARY_COLUMNS['SCOPE_CHANGES'], 'Scope Changes'
        )
        if scope_changes_df is not None:
            if selected_duration in scope_changes_df.columns:
                styled_df = scope_changes_df.style.set_properties(subset=[selected_duration], **{'background-color': 'rgba(173, 216, 230, 0.4)'})
                st.dataframe(styled_df, hide_index=True, use_container_width=True)
            else:
                st.dataframe(scope_changes_df, hide_index=True, use_container_width=True)