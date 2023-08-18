import requests
from requests.auth import HTTPBasicAuth
import json
import pandas as pd
from ast import literal_eval
import numpy as np
import ast
import plotly.express as px
import matplotlib.pyplot as plt

COLOR_TASK = 'dodgerblue'
COLOR_STORY = 'green'
COLOR_SUBTASK = 'cyan'
COLOR_BUG = 'red'
COLOR_EPIC = 'darkmagenta'

COLOR_DICT = {
    'purple':'purple',
    'dark_blue':'darkblue',
    'yellow':'orange',
    'grey':'grey',
    'dark_purple':'purple',
    'blue':'blue',
    'dark_orange':'orange',
    'dark_yellow':'gold',
    'blue-gray':'dodgerblue',
    'green':'green'
}


url = "https://globl.atlassian.net/rest/api/2/search"
auth = HTTPBasicAuth("", "")
headers = {"Accept": "application/json"}
query = {'jql': 'project = GLOB'}

response = requests.request("GET", url, headers=headers, auth=auth, params=query)

projectIssues = json.dumps(json.loads(response.text),
                            sort_keys=True,
                            indent=4,
                            separators=(",", ": "))

dictProjectIssues = json.loads(projectIssues)

list_all_issues = []


def iterate_dict_issues(outer_issues, inner_list):
    field_mappings = {
        "key": "Key",
        "summary": "Summary",
        "reporter": "Reporter",
        "creator": "Creator",
        "customfield_10016": "Customfield_10016",
        "customfield_10020": "Customfield_10020",
        "issuetype": "Issuetype",
        "priority": "Priority",
        "project": "Project",
    }

    for key, values in outer_issues.items():
        if key == "fields":
            fields_dict = dict(values)
            iterate_dict_issues(fields_dict, inner_list)
        elif key in field_mappings:
            inner_list.append(values)
        else:
            inner_list.append(None)

for key, value in dict_project_issues.items():
    if key == "issues":
        for issue in value:
            inner_list = []
            iterate_dict_issues(issue, inner_list)
            list_all_issues.append(inner_list)

df_issues = pd.DataFrame(list_all_issues, columns=[
    "Key", "Summary", "Reporter", "Creator",
    "Customfield_10016", "Customfield_10020",
    "Issuetype", "Priority", "Project"
])

column_titles = [
    "Key", "Summary", "Reporter", "Creator",
    "Customfield_10016", "Customfield_10020",
    "Issuetype", "Priority", "Project"
]
df_issues = df_issues[column_titles]

dfIssues.to_csv('jiraIssues.csv', index=False)

df = pd.read_csv("jiraIssues.csv")

df.Customfield_10020 = df.Customfield_10020.fillna('{}')
df.Customfield_10020 = df.Customfield_10020.apply(lambda x: ast.literal_eval(x) if isinstance(x, str) else x)
df = df.join(pd.json_normalize(df.pop('Customfield_10020')))

df1 = df.rename(columns={0: 'city'})

df2 = df1.join(pd.DataFrame(df1.pop('city').values.tolist()))

df2 = df2.rename(columns={'Key': 'issue_key', 'Summary': 'issue_summary', 'Project': 'project_name', 'Issuetype': 'issuetype', 'Priority': 'priority', 'Customfield_10016': 'storypoints', 'name': 'sprint_name', 'startDate': 'sprint_start_date', 'endDate': 'sprint_end_date', 'state': 'sprint_state', 'completeDate': 'sprint_completed_date'})

df2["storypoints"].fillna(8, inplace = True)

index = 0
col = "project_name"
project_name = df2.iloc[index][col]
print("Cell value at ", index, "for column ", col, " : ", project_name)

df2.drop(columns=['Reporter', 'ProjectId', 'Creator', 'boardId', 'goal', 'id'], inplace=True)

df2 = df2.drop_duplicates()

df_issues_per_sprint = df2.query("project_name == `project_name`")[['project_name','issuetype','priority','storypoints','sprint_name','sprint_start_date','sprint_end_date']].groupby(['project_name','issuetype','priority','sprint_name','sprint_start_date','sprint_end_date'])['storypoints'].agg(storypoints='sum').reset_index()

df_issues_per_sprint.to_csv('jiraIssues_modified.csv', index=False)

fig = px.histogram(df_issues_per_sprint, x="sprint_name", y="storypoints", color='issuetype',
    title='Storypoints per Issue Type',
    labels={'sprint_name':'Sprint Name','issue_count':'Number of Issues','issuetype':'Issue Type'},
    color_discrete_map={
        "Story": COLOR_STORY,
        "Bug": COLOR_BUG,
        "Task": COLOR_TASK,
        "Sub-task": COLOR_SUBTASK})

fig.update_layout(
    yaxis_title="Storypoints",
    xaxis_title="Sprints",
)

fig.write_image('sprint_velocity.png')
fig.show()

sprints = df2.loc[df2['sprint_state'] == 'closed']

sprint_issue_counts = sprints['sprint_name'].value_counts().sort_index()

sprints['sprint_velocity'] = sprints['storypoints']
sprint_velocities = sprints.groupby('sprint_name')['sprint_velocity'].sum().sort_index()

average_velocity = sprint_velocities.mean()

sprints['sprint_velocities'] = sprints['sprint_name'].map(sprint_velocities)
sprints['average_sprint_velocity'] = average_velocity

df2['sprint_velocities'] = df2['sprint_name'].map(sprint_velocities)

df2['average_sprint_velocity'] = average_velocity

df2.loc[df2['sprint_velocities'].isnull(), 'average_sprint_velocity'] = ''

df2 = df2.sort_values(by="sprint_name", ascending=True)

df2 = df2.loc[:, ['issue_key', 'issue_summary', 'project_name', 'priority', 'issuetype', 'storypoints', 'sprint_name',  'sprint_start_date', 'sprint_end_date', 'sprint_state', 'sprint_completed_date', 'sprint_velocities', 'average_sprint_velocity']]

df2.to_csv('sprintvelocitydataset.csv', index=False)