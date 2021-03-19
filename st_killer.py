import boto3, requests, json, re
import logging, os, tzlocal
from datetime import datetime, timedelta
from tzlocal import get_localzone

from requests.exceptions import HTTPError
from botocore.config import Config
from boto3.session import Session

slack_token   = os.environ["SLACK_API_TOKEN"]
slack_webhook = os.environ["SLACK_WEBHOOK"]
slack_channel = os.environ["SLACK_CHANNEL"]

logging.basicConfig(level=logging.INFO)

config = Config(
    retries = dict(
        max_attempts = 20
    )
)

lookup_url="https://slack.com/api/users.lookupByEmail?token="+slack_token+"&email="
owners_url="https://slack.com/api/chat.postMessage?token="+slack_token+"&channel="
#List of dictionaries containing each ECS cluster and its region we will search
#Example:
#clusters = [{'name': 'Cluster-1', 'region': 'us-east-1'},
#            {'name': 'Cluster-2', 'region': 'us-west-1'}
#]
clusters = []

#If clusters are spread over several accounts, include an STS block for each
#Check README.md for details
def setup_client(cluster, region_name):
    global client_ecs
    if cluster == 'Cluster-in-separate-account':
        sts = boto3.client('sts')
        credentials = sts.assume_role(
            RoleArn="arn:aws:iam::<account>:role/Role-ecs-st-killer",
            RoleSessionName="ecs_STS"
        )
        session = Session(
                aws_access_key_id=credentials['Credentials']['AccessKeyId'],
                aws_secret_access_key=credentials['Credentials']['SecretAccessKey'],
                aws_session_token=credentials['Credentials']['SessionToken']
        )
        client_ecs = session.client('ecs',region_name=region_name, config=config)
    else:
        client_ecs = boto3.client('ecs', region_name=region_name, config=config)

def get_tasks(cluster):
    tasks = []
    extraArgs = {}
    while True:
        try:
            response = client_ecs.list_tasks(cluster=cluster,
                                              launchType='FARGATE',**extraArgs)
        except Exception as e:
            raise
        else:
            tasks.append(response['taskArns'])
            if 'nextToken' in response:
                extraArgs['nextToken'] = response['nextToken']
            else:
                break
    return tasks

def describe_tasks(cluster, tasks):
    detail_tasks = []
    extraArgs = {}
    i = 0
    while i < len(tasks):
        try:
            response = client_ecs.describe_tasks(cluster=cluster,tasks=tasks[i])
        except Exception as e:
            raise
        else:
            j = 0
            while j < len(response['tasks']):
                try:
                    task = [response['tasks'][j]['startedAt'],
                            response['tasks'][j]['taskArn'],
                            response['tasks'][j]['taskDefinitionArn']]
                    detail_tasks.append(task)
                except Exception as e:
                    pass
                j += 1
            i += 1
    return detail_tasks

def task_is_previous_version(task, task_description):
    task_arn = task.rpartition(":")[0]
    revision = int(task.rpartition(":")[2])
    if task_description['taskDefinition']['revision'] > revision:
        return True
    else:
        return False

def task_is_old(then):
    tz = get_localzone()
    now = datetime.now(tz)
    if (now - then) > timedelta(hours=3):
        return True
    else:
        return False

def kill_task(cluster, task):
    response = client_ecs.stop_task(cluster=cluster, task=task,
                                      reason='dangling task - ST killer')

def find_task_owners(task_description):
    try:
        owners = task_description['taskDefinition']['containerDefinitions'][0]['dockerLabels']['owner']
        rep = {"[u'":"", " u'":"", "'":"", "]":""}
        rep = dict((re.escape(k), v) for k, v in rep.items())
        pattern = re.compile("|".join(rep.keys()))
        owners = pattern.sub(lambda m: rep[re.escape(m.group(0))], owners)
        list_owners = owners.split(",")
    except:
        list_owners = ['None']
    return list_owners

def find_owners_in_slack(owners):
    found = False
    slack_owners = []
    for i in owners:
        mail_url = lookup_url + i + "@domain.com"
        try:
            response = requests.get(mail_url)
        except Exception as e:
            raise
        else:
            response_json = json.loads(response.text)
            if response_json['ok']:
                slack_owners.append(response_json['user']['id'])
                found = True
    return slack_owners, found

def notify_owners_in_slack(cluster, task, task_name, ms_name, slack_owners):
    headers = {
        'Content-Type': 'application/x-www-form-urlencoded'
    }
    payload = {}
    message="La task: " + task_name + " (" + task + ") del MS: " + ms_name + " en el cluster: " + cluster + " ha sido _eliminada._ "
    web_message = message.replace(" ", "%20")
    for i in slack_owners:
        dm_url = owners_url + i + "&text=" + web_message
        try:
            response = requests.post(dm_url, headers=headers)
        except Exception as e:
            raise

def notify_channel_in_slack(cluster, task, task_name, ms_name, owners, found):
    headers = {
        'Accept': '/',
        'Content-Type': 'application/json'
    }
    payload = {}
    payload['channel'] = slack_channel
    message="La task: " + task_name + " (" + task + ") del MS: " + ms_name + " en el cluster: " + cluster + " ha sido _eliminada._ "
    message= message + "Los owners de la task son: " + str(owners)
    if not found:
        message = message + " - *WARNING*: NO pude contactar a los owners por mensaje directo!"
    payload['text'] = message
    try:
        response = requests.post(slack_webhook, data=json.dumps(payload),
                                    headers=headers)
    except Exception as e:
        raise

def notify_start_killing(cluster):
    headers = {
        'Accept': '/',
        'Content-Type': 'application/json'
    }
    payload = {}
    payload['channel'] = slack_channel
    message="Comienzo la matanza en el cluster: " + cluster
    payload['text'] = message
    try:
        response = requests.post(slack_webhook, data=json.dumps(payload),
                                    headers=headers)
    except Exception as e:
        raise

def handler(event, context):
    #main
    for cluster in clusters:
        setup_client(cluster['name'], cluster['region'])
        notify_start_killing(cluster['name'])
        tasks = get_tasks(cluster['name'])
        detail = describe_tasks(cluster['name'], tasks)
        for i in detail:
            task_description = client_ecs.describe_task_definition(taskDefinition=i[2])
            if task_is_previous_version(i[2], task_description) or task_is_old(i[0]):
                kill_task(cluster['name'], i[1])
                owners = find_task_owners(task_description)
                slack_owners, found = find_owners_in_slack(owners)
                ms_name = i[2].rpartition("/")[2].rpartition(":")[0]
                task_name = task_description['taskDefinition']['containerDefinitions'][0]['logConfiguration']['options']['awslogs-stream-prefix']
                if found:
                    notify_owners_in_slack(cluster['name'], i[1], task_name, ms_name, slack_owners)
                notify_channel_in_slack(cluster['name'], i[1], task_name, ms_name, owners, found)
