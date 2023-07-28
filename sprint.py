import requests
from requests.auth import HTTPBasicAuth
import json
import pandas as pd
import pandas as pd
from ast import literal_eval
import numpy as np
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

response = requests.request("GET", url, headers=headers, auth=auth, params=query)

projectIssues = json.dumps(json.loads(response.text),
                            sort_keys=True,
                            indent=4,
                            separators=(",", ": "))

dictProjectIssues = json.loads(projectIssues)

listAllIssues = []

keyIssue, keySummary, keyReporter, keyCreator, keyCustomfield_10033, keyCustomfield_10020, keyIssuetype, keyPriority, keyProject = "", "", "", "", "", "", "", "",""


def iterateDictIssues(oIssues, listInner):
    for key, values in oIssues.items():
        print(key, values)
        if(key == "fields"):
            fieldsDict = dict(values)
            iterateDictIssues(fieldsDict, listInner)
        elif (key == "reporter"):
            reporterDict = dict(values)
            iterateDictIssues(reporterDict, listInner)
        elif (key == "creator"):
            creatorDict = dict(values)
            iterateDictIssues(creatorDict, listInner)
        elif (key == "issuetype"):
            issuetypeDict = dict(values)
            iterateDictIssues(issuetypeDict, listInner)
        elif (key == "priority"):
            priorityDict = dict(values)
            iterateDictIssues(priorityDict, listInner)
        elif (key == "project"):
            projectDict = dict(values)
            iterateDictIssues(projectDict, listInner)
        elif(key == 'key'):
            keyIssue = values
            listInner.append(keyIssue)
        elif(key == 'summary'):
            keySummary = values
            listInner.append(keySummary)
        elif(key == "displayName"):
            keyReporter = values
            listInner.append(keyReporter)
        elif(key == "displayName"):
            keyCreator = values
            listInner.append(keyCreator)
        elif(key == "customfield_10033"):
            keyCustomfield_10033 = values
            listInner.append(keyCustomfield_10033)
        elif(key == "customfield_10020"):
            keyCustomfield_10020 = values
            listInner.append(keyCustomfield_10020)
        elif(key == "name"):
            keyIssuetype = values
            listInner.append(keyIssuetype)
        elif(key == "name"):
            keyPriority = values
            listInner.append(keyPriority)
        elif(key == "name"):
            keyProject = values
            listInner.append(keyProject)

for key, value in dictProjectIssues.items():
    if(key == "issues"):
        totalIssues = len(value)
        for eachIssue in range(totalIssues):
            listInner = []
            iterateDictIssues(value[eachIssue], listInner)
            listAllIssues.append(listInner)

dfIssues = pd.DataFrame(listAllIssues, columns=["Creator", "Customfield_10020", "Customfield_10033", "Issuetype", "Priority", "ProjectId", "Project", "Reporter", "Summary", "Key"])

columnTiles = ["Key", "Summary", "Reporter", "Project", "ProjectId", "Priority", "Issuetype", "Customfield_10033", "Customfield_10020", "Creator"]

dfIssues = dfIssues.reindex(columns=columnTiles)

dfIssues.to_csv('jiraIssues.csv', index=False)

df = pd.read_csv("jiraIssues.csv")

# replace NaN with '{}' if the column is strings, otherwise replace with {}
df.Customfield_10020 = df.Customfield_10020.fillna('{}')  # if the NaN is in a column of strings
# df.Pollutants = df.Pollutants.fillna({i: {} for i in df.index})  # if the column is not strings

# Convert the column of stringified dicts to dicts
# skip this line, if the column contains dicts
df.Customfield_10020 = df.Customfield_10020.apply(literal_eval)

# reset the index if the index is not unique integers from 0 to n-1
# df.reset_index(inplace=True)  # uncomment if needed

# remove and normalize the column of dictionaries, and join the result to df
df = df.join(pd.json_normalize(df.pop('Customfield_10020')))

df1 = df.rename(columns={0: 'city'})

df2 = df1.join(pd.DataFrame(df1.pop('city').values.tolist()))

df2 = df2.rename(columns={'Key': 'issue_key', 'Summary': 'issue_summary', 'Project': 'project_name', 'Issuetype': 'issuetype', 'Priority': 'priority', 'Customfield_10033': 'storypoints', 'name': 'sprint_name', 'startDate': 'sprint_start_date', 'endDate': 'sprint_end_date', 'state': 'sprint_state', 'completeDate': 'sprint_completed_date'})

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

# fig.write_image('sprint_velocity.png')
# fig.show()

sprints = df2.loc[df2['sprint_state'] == 'closed']

# Calculate total number of issues for each sprint
sprint_issue_counts = sprints['sprint_name'].value_counts().sort_index()

# Calculate sprint velocities
sprints['sprint_velocity'] = sprints['storypoints']
sprint_velocities = sprints.groupby('sprint_name')['sprint_velocity'].sum().sort_index()

# Calculate average sprint velocity
average_velocity = sprint_velocities.mean()

# Insert sprint velocities into the dataset
sprints['sprint_velocities'] = sprints['sprint_name'].map(sprint_velocities)

# Insert average sprint velocity into the dataset
sprints['average_sprint_velocity'] = average_velocity

# Insert sprint velocities into the dataset
sprints['sprint_velocities'] = sprints['sprint_name'].map(sprint_velocities)

# Insert average sprint velocity into the dataset
sprints['average_sprint_velocity'] = average_velocity

# Insert sprint velocities into the dataset
df2['sprint_velocities'] = df2['sprint_name'].map(sprint_velocities)

# Insert average sprint velocity into the dataset
df2['average_sprint_velocity'] = average_velocity

df2.loc[df2['sprint_velocities'].isnull(), 'average_sprint_velocity'] = ''

df2 = df2.sort_values(by="sprint_name", ascending=True)

df2 = df2.loc[:, ['issue_key', 'issue_summary', 'project_name', 'priority', 'issuetype', 'storypoints', 'sprint_name',  'sprint_start_date', 'sprint_end_date', 'sprint_state', 'sprint_completed_date', 'sprint_velocities', 'average_sprint_velocity']]

df2.to_csv('sprintvelocitydataset.csv', index=False)
