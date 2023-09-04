try:
    import datetime
    import pytz
    from github import Github
    import requests
    from flask import Flask, request, jsonify, render_template
    print("All imports ok ...")
except Exception as e:
    print("Error Imports : {} ".format(e))

app = Flask(__name__)


# @cross_origin(origins='*')
@app.route('/githublinesofcode', methods=['POST', 'GET'])
def githublinesofcode():
    try:
        _json = request.json
        REPO_OWNER = _json['REPO_OWNER']
        REPO_NAME = _json['REPO_NAME']
        ACCESS_TOKEN = _json['ACCESS_TOKEN']
        # GitHub access token

        # Initialize the GitHub object
        g = Github(ACCESS_TOKEN)
        repo = g.get_repo(f'{REPO_OWNER}/{REPO_NAME}')
       
        # Calculate the start and end dates of the current week
        today = datetime.date.today()
        start_of_week = today - datetime.timedelta(days=today.weekday())
        end_of_week = start_of_week + datetime.timedelta(days=6)

        # Converting start and end dates to UTC timezone
        utc = pytz.UTC
        start_of_week = datetime.datetime.combine(start_of_week, datetime.time()).replace(tzinfo=utc)
        end_of_week = datetime.datetime.combine(end_of_week, datetime.time()).replace(tzinfo=utc)

        # Initialize a dictionary to store the lines of code per user
        lines_of_code_per_user = {}

        # Iterating over all commits in the repository
        commits = repo.get_commits(since=start_of_week, until=end_of_week)
        for commit in commits:
            author = commit.commit.author.name
            stats = commit.stats
            added_lines = stats.additions
            deleted_lines = stats.deletions
            total_lines_changed = added_lines + deleted_lines

            if author in lines_of_code_per_user:
                lines_of_code_per_user[author] += total_lines_changed
            else:
                lines_of_code_per_user[author] = total_lines_changed
        
        # Total lines of code in the Git Repository
        url = f"https://api.github.com/repos/{REPO_OWNER}/{REPO_NAME}/stats/code_frequency"
        response = requests.get(url)

        outputData = {}
        for user, lines_changed in lines_of_code_per_user.items():
            outputData['user'] = user
            outputData['lines_changed'] = lines_changed
            # outputData.append({'user': user, 'lines_changed': lines_changed})
            if response.status_code == 200:
                data = response.json()
                total_lines_of_code = sum([additions + deletions for _, additions, deletions in data])
                outputData['Repo_total_lines'] = total_lines_of_code
            else:
                print(f"Failed to retrieve repository stats. Error: {response.status_code}")

        # print(outputData)

    except Exception as e:
        print(e)
    finally:
        return outputData


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)
