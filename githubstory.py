from datetime import datetime, timedelta
from github import Github
from github import Auth


# Repository details
repository_owner = 'digitivy-inc'
repository_name = 'prismpodml'

# Number of days to go back to calculate lines of code
days_to_go_back = 7

# Create a PyGithub instance
# g = Github(access_token)
auth = Auth.Token(access_token)
g = Github(auth=auth)

# Get the repository
# repository = g.get_repo(f"{repository_owner}/{repository_name}")
# repository = Github(auth=auth, base_url="https://{hostname}/api/v3")

# Get the starting and ending dates for the week
end_date = datetime.now()
start_date = end_date - timedelta(days=days_to_go_back)

# Iterate over the contributors
contributors = repository.get_contributors()
for contributor in contributors:
    # Get the commits made by the contributor within the specified date range
    commits = repository.get_commits(author=contributor.login, since=start_date, until=end_date)
    lines_added = 0
    lines_deleted = 0

    # Count the lines of code added and deleted in each commit
    for commit in commits:
        diff = repository.compare(commit.parents[0].sha, commit.sha)
        for file in diff.files:
            lines_added += file.additions
            lines_deleted += file.deletions

    # Print the statistics for the contributor
    print(f"Contributor: {contributor.login}")
    print(f"Lines added: {lines_added}")
    print(f"Lines deleted: {lines_deleted}")
    print(f"Total lines changed: {lines_added + lines_deleted}")
    print("------------------------------")
