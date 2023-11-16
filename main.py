import requests
import json
import dotenv
import os
import time

def run_databricks_job(token, hosturl, jobid):  
    '''
    Run the Databricks workflow with the specified job ID.
    This specifically analyzes stock data from Dow Jones Industrial Average (DJIA)
    by running the files in ETL-notebooks
    '''
    # set bearer token authentication header
    headers = {
        'Authorization': f'Bearer {token}',
        'Content-Type': 'application/json'
    }

    # specify the job parameters as needed
    data = {
        'job_id': jobid,
    }

    # set up the API endpoint url
    api_url = f"https://{hosturl}/api/2.0/jobs/run-now"
    # submit the job run
    response = requests.post(api_url, headers=headers, json=data, timeout=120)

    if response.status_code != 200:
        print(f'Error: {response.status_code}, {response.text}')
    else:
        run_id = response.json()['run_id']
        print(f'Successfully submitted job run with ID {run_id}')

        while True:
            # check job run result status
            result_url = f'https://{hosturl}/api/2.0/jobs/runs/get?run_id={run_id}'
            response = requests.get(result_url, headers=headers)
            # error handling
            if response.status_code != 200:
                print(f'Error getting job run result: {response.json()}')
                break
            else:
                state = response.json()['state']['life_cycle_state']
                print(f"Job run {run_id} has state: {state}")
                # when job is done print if successful or not
                if state == "TERMINATED":
                    result_state = response.json()["state"]["result_state"]
                    if result_state == "SUCCESS":
                        print(f"Job run {run_id} succeeded!")
                        break
                    else:
                        print(f"Job run {run_id} failed with result state: {result_state}")
                        break
            time.sleep(4)
                

if __name__ == '__main__':
    # load personal access token from .env file
    dotenv.load_dotenv()
    personal_access_token = os.getenv('DATABRICKS_KEY')
    base_url = os.getenv('DATABRICKS_HOST')
    job_id = os.getenv('DATABRICKS_JOBID')

    # run the job
    run_databricks_job(personal_access_token, base_url, job_id)