import requests
from sklearn.linear_model import LinearRegression


# GitHub API endpoint for retrieving repository statistics
API_URL = "https://api.github.com/repos/{owner}/{repo}/stats/contributors"


def get_total_commits_per_week(owner, repo):
    """
    Retrieves the total number of commits per week for a given repository.
    """
    # Retrieve statistics for the given repository
    response = requests.get(API_URL.format(owner=owner, repo=repo))
    data = response.json()
    return data


def get_total_lines_of_code(owner, repo):
    # Make a GET request to the GitHub API
    api_endpoint = API_URL.format(owner=owner, repo=repo)
    response = requests.get(api_endpoint)
    
    if response.status_code == 200:
        data = response.json()
        total_lines_of_code = 0
        
        # Extract the lines of code from the API response
        for contributor in data:
            for week in contributor['weeks']:
                total_lines_of_code += week['a'] + week['d']
        
        return total_lines_of_code
    else:
        print(f"Failed to fetch repository statistics: {response.status_code}")


def get_total_lines_of_code1(owner, repo, api_token):
    # Make a GET request to the GitHub API
    api_endpoint = API_URL.format(owner=owner, repo=repo)
    headers = {'Authorization': f'token {api_token}'}
    response = requests.get(api_endpoint, headers=headers)
    
    if response.status_code == 200:
        data = response.json()
        total_lines_of_code = 0
        
        # Extract the lines of code from the API response
        for contributor in data:
            for week in contributor['weeks']:
                total_lines_of_code += week['a'] + week['d']
        
        return total_lines_of_code
    else:
        print(f"Failed to fetch repository statistics: {response.status_code}")


def get_total_commits(owner, repo):
    # Make a GET request to the GitHub API
    api_endpoint = API_URL.format(owner=owner, repo=repo)
    response = requests.get(api_endpoint)
    
    if response.status_code == 200:
        data = response.json()
        total_commits = 0
        
        # Extract the commits from the API response
        for contributor in data:
            total_commits += contributor['total']
        
        return total_commits
    else:
        print(f"Failed to fetch repository statistics: {response.status_code}")


def get_total_pull_requests(owner, repo):
    # Make a GET request to the GitHub API
    api_endpoint = API_URL.format(owner=owner, repo=repo)
    response = requests.get(api_endpoint)
    
    if response.status_code == 200:
        data = response.json()
        total_pull_requests = 0
        
        # Extract the pull requests from the API response
        for contributor in data:
            total_pull_requests += contributor['total']
        
        return total_pull_requests
    else:
        print(f"Failed to fetch repository statistics: {response.status_code}")


def get_total_issues(owner, repo):
    # Make a GET request to the GitHub API
    api_endpoint = API_URL.format(owner=owner, repo=repo)
    response = requests.get(api_endpoint)
    
    if response.status_code == 200:
        data = response.json()
        total_issues = 0
        
        # Extract the issues from the API response
        for contributor in data:
            total_issues += contributor['total']
        
        return total_issues
    else:
        print(f"Failed to fetch repository statistics: {response.status_code}")


# Example usage
repository_owner = "your_username"
repository_name = "your_repository_name"

total_lines = get_total_lines_of_code(repository_owner, repository_name)
print(f"Total lines of code: {total_lines}")
