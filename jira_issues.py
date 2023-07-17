import requests
from datetime import datetime, timedelta


# Get the current date and the start of the week (Monday)
current_date = datetime.now().date()
start_of_week = current_date - timedelta(days=current_date.weekday())

# Jira query to fetch the user story and defect details
jql = f'project =  AND (issuetype = "User Story" OR issuetype = Defect)'

# Jira API request headers
headers = {
    'Content-Type': 'application/json',
}

# Function to retrieve Jira statistics per user


def get_jira_stats():
    # Get all Jira issues matching the JQL query
    issues_url = f'{jira_url}/rest/api/2/search'
    params = {
        'jql': jql,
        'fields': 'status,issuetype,assignee,resolutiondate,created',
        'maxResults': 1000
    }
    response = requests.get(issues_url, auth=(
        jira_user, jira_api_token), headers=headers, params=params)
    issues = response.json()['issues']

    # Dictionary to store user-wise statistics
    user_stats = {}

    # Iterate through each Jira issue
    for issue in issues:
        assignee = issue['fields']['assignee']['displayName']
        status = issue['fields']['status']['name']
        created_date = datetime.strptime(
            issue['fields']['created'], '%Y-%m-%dT%H:%M:%S.%f%z').date()
        resolution_date = None

        # Check if the issue has been resolved
        if issue['fields'].get('resolutiondate'):
            resolution_date = datetime.strptime(
                issue['fields']['resolutiondate'], '%Y-%m-%dT%H:%M:%S.%f%z').date()

        # Increment the appropriate statistics for the assignee
        if assignee not in user_stats:
            user_stats[assignee] = {'completed': 0,
                                    'open_defects': 0, 'resolved_this_week': 0}

        if status == 'Done' and resolution_date and created_date >= start_of_week:
            user_stats[assignee]['completed'] += 1
        elif issue['fields']['issuetype']['name'] == 'Defect':
            if status == 'Open':
                user_stats[assignee]['open_defects'] += 1
            elif status == 'Done' and resolution_date and resolution_date >= start_of_week:
                user_stats[assignee]['resolved_this_week'] += 1

    return user_stats


# Get the Jira statistics
stats = get_jira_stats()

# Print the statistics per user
for user, data in stats.items():
    print(f'User: {user}')
    print(f'Completed User Stories: {data["completed"]}')
    print(f'Open Defects: {data["open_defects"]}')
    print(f'Defects Resolved (Done) This Week: {data["resolved_this_week"]}')
    print('---')
