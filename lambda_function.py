import requests
import json
import pandas as pd
import pandasql as ps
from datetime import datetime

def get_circleci_deployments(api_token, username, project):
    base_url = "https://circleci.com/api/v1.1"
    headers = {
        "Accept": "application/json",
        "Circle-Token": api_token,
    }
    url = f"{base_url}/project/github/{username}/{project}"
    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        pipeline_data = response.json()
        deployments = []
        for pipeline in pipeline_data:
            pipeline_number = pipeline["build_num"]
            workflow_name = pipeline["workflows"]["workflow_name"]
            deployment_status = pipeline["status"]
            deployment_date = pipeline["start_time"]

            deployments.append({
                "pipeline_number": pipeline_number,
                "workflow_name": workflow_name,
                "deployment_status": deployment_status,
                "deployment_date": deployment_date,
            })
        return deployments
    except requests.exceptions.RequestException as e:
        print(f"Error: {e}")
        return None

def run_code(event=None, context=None):
    outputData = {}
    try:
        # Circle CI login credentials
        username = event.get('github_username')
        project = event.get('github_repo_or_project_name')
        api_token = event.get('circleci_api_token')
        
        deployments = get_circleci_deployments(api_token, username, project)

        # Convert deployments to a pandas DataFrame
        deploy_df = pd.DataFrame(deployments)
        deploy_df1 = pd.DataFrame(deployments)

        # Display the DataFrame
        deploy_df1 = deploy_df1.loc[deploy_df1['deployment_status'] == 'success']

        deploy_df['deployment_status'] = deploy_df['deployment_status'].replace({'failed': 'False', 'success': 'True'})
        
        # Convert 'deployment_date' column to datetime objects
        deploy_df['deployment_date'] = pd.to_datetime(deploy_df['deployment_date'])

        # Convert 'deployment_status' column to boolean
        deploy_df['deployment_status'] = deploy_df['deployment_status'].map({'True': True, 'False': False})

        # Calculate deployment count and total days
        deployment_count = deploy_df['deployment_status'].sum()
        total_days = (deploy_df['deployment_date'].max() - deploy_df['deployment_date'].min()).days + 1

        # Calculate deployment frequency
        deployment_frequency = deployment_count / total_days
        
        # SQL query to retrieve the required data
        query = '''
            SELECT pipeline_number, workflow_name, deployment_status, deployment_date
            FROM deploy_df1
        '''

        # Execute the SQL query using pandasql
        deployments_df = ps.sqldf(query, locals())

        # Drop duplicate rows if any
        deployments_df.drop_duplicates(inplace=True)

        # Convert the DataFrame to a dictionary
        deployments_data = deployments_df.to_dict(orient='records')

        # Convert deployment_frequency to string
        deployment_frequency_str = str(deployment_frequency)
        output1 = "Deployment Frequency: " + str(deployment_frequency) + " deployments per day"
        output = {}
        output['successful_deployments'] = deployment_frequency
        output['deployment_fequency'] = output1

        # Prepare the response data as a dictionary
        outputData['deployment_details'] = deployments_data
        outputData['deployments'] = output

        return {
            'statusCode': 200,
            'body': json.dumps(outputData)
        }

    except ValueError as ve:
        print(f"ValueError: {ve}")
        return {
            'statusCode': 500,
            'body': json.dumps({'error': str(ve)})
        }


def lambda_handler(event, context):
    return run_code(event, context)