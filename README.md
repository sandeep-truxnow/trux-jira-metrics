# trux-jira-metrics
This repo will be used for leadership and teams to understand the sprint progress and blockers in terms of cycle time and lead time


  
## Breaking it down:

 ### A. completed_stories - First, it filters to only include completed issues:
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

## B. avg_sprints_per_story
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
