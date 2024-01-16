#!/usr/bin/env python3

import os
import sys
import json
import yaml
import uuid
import time
import urllib3
import pathlib
import jenkins
import logging
import argparse
import requests
import datetime
import subprocess
import coloredlogs
from elasticsearch import Elasticsearch


# disable SSL and warnings
os.environ['PYTHONHTTPSVERIFY'] = '0'
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# directory constants 
ROOT_DIR = str(pathlib.Path(__file__).parent.parent)
SCRIPT_DIR = ROOT_DIR + '/scripts'
DATA_DIR = ROOT_DIR + '/data'

class DataHandler():
    def __init__(self) -> None:
        self.name = 'DataHandler'
        self.data = {}
        self.results = { "data": [] }
        self.es_url = 'search-ocp-qe-perf-scale-test-elk-hcm7wtsqpxy7xogbu72bor4uve.us-east-1.es.amazonaws.com'
        self.es_username = os.getenv('ES_USERNAME')
        self.es_password = os.getenv('ES_PASSWORD')

    def get_iso_timestamp(unix_timestamp):
        ''' takes in a unix timestamp and returns an iso timestamp representation
            iso timestamp will be Elasticsearch compatible 
        '''
        return datetime.datetime.utcfromtimestamp(int(unix_timestamp)).isoformat() + 'Z'

    def run_commands(commands, outputs={}):
        ''' executes dictionary of commands where key is identifier and value is command
            raises Exception if command fails
            returns outputs dictionary, either new or appended to passed dict
        '''

        # iterate through commands dictionary
        for command in commands:
            logging.debug(f"Executing command '{commands[command]}' to get {command} data")
            try:
                # execute next command in iteration
                result = subprocess.run(commands[command], capture_output=True, text=True, shell=True, timeout=600)

                # record command stdout if execution was succesful
                output = result.stdout
                logging.debug(f"Got back result: {output}")

                # if aws_s3_bucket_usage command, split up the data to two different metadata fields
                if command == 'aws_s3_bucket_usage':
                    aws_data = output.replace(" ","").splitlines()
                    aws_buckets = aws_data[0].split(':', 1)[1]
                    aws_size = aws_data[1].split(':', 1)[1]
                    logging.debug(f"aws_s3_bucket_objects calculated as {aws_buckets}")
                    outputs['aws_s3_bucket_objects'] = aws_buckets
                    logging.debug(f"aws_s3_bucket_size calculated as {aws_size}")
                    outputs['aws_s3_bucket_size'] = aws_size

                # otherwise map command output to dict entry
                else:
                    outputs[command] = output

            except Exception as e:
                # if command failed but was AWS-specific, don't block, just continue
                if 'aws' in command:
                    output = result.stderr
                    logging.error(f"Got back result: {output} from AWS command - skipping...")
                    outputs[command] = "N/A"

                # otherwise, log the error and exit
                else:
                    logging.error(f"Command '{command}' execution resulted in stderr output: {e}")
                    sys.exit(1)

        # if all commands were successful return outputs dictionary
        return outputs

    def dump_data_locally(self, timestamp, partial=False):
        ''' writes captured data in self.results dictionary to a JSON file
            file is saved to 'data_{timestamp}.json' in DATA_DIR system path if data is complete
            file is saved to 'partial_data_{timestamp}.json' in DATA_DIR system path if data is incomplete
        '''

        # ensure data directory exists (create if not)
        pathlib.Path(DATA_DIR).mkdir(parents=True, exist_ok=True)

        # write prometheus data to data directory
        if not partial:
            with open(DATA_DIR + f'/data_{self.name}_{timestamp}.json', 'w') as data_file:
                json.dump(self.results, data_file)
        else:
            with open(DATA_DIR + f'/partial_data_{self.name}_{timestamp}.json', 'w') as data_file:
                json.dump(self.results, data_file)

        # return if no issues
        return None

    def upload_data_to_elasticsearch(self):
        ''' uploads captured data in self.results dictionary to Elasticsearch
        '''

        # create Elasticsearch object and attempt index
        es = Elasticsearch(
            [f'https://{self.es_username}:{self.es_password}@{self.es_url}:443'],
            timeout=30,
            max_retries=10,
            retry_on_timeout=True
        )

        start = time.time()
        for item in self.results['data']:
            metric_name = item.get('metric_name')
            if metric_name == 'netobservEnv':
                index = 'prod-netobserv-operator-metadata'
            elif metric_name == 'jenkinsEnv':
                index = 'prod-netobserv-jenkins-metadata'
            else:
                index = 'prod-netobserv-datapoints'
            logging.debug(f"Uploading item '{item}' to index '{index}' in Elasticsearch")
            response = es.index(
                index=index,
                body=item
            )
            logging.debug(f"Response back was {response}")
        end = time.time()
        elapsed_time = end - start

        # close Elasticsearch object and return elapsed time for upload if no issues
        es.close()
        return elapsed_time


class Netobserv(DataHandler):
    def __init__(self, uuid, start_time, jira) -> None:
        super().__init__()
        self.name = 'Netobserv'
        self.uuid = uuid
        self.start_time = start_time
        self.jira = jira

    def collect(self):
        ''' gathers information about netobserv operator env
            returns info dictionary where key is identifier and value is command output
        '''

        # intialize info and base_commands objects
        iso_timestamp = self.get_iso_timestamp(self.start_time)
        self.data = {
            "uuid": self.uuid,
            "jira": self.jira,
            "metric_name": "netobservEnv",
            "data_type": "metadata",
            "iso_timestamp": iso_timestamp,
        }
        base_commands = {
            "ocp": 'oc get co/authentication -o=jsonpath="{.status.versions[0].version}"',
            "release": 'oc get pods -l app=netobserv-operator -o jsonpath="{.items[*].spec.containers[1].env[0].value}" -A',
            "arch": 'oc get node -o jsonpath="{.items[0].status.nodeInfo.architecture}"',
            "loki": 'oc get sub -n openshift-operators-redhat loki-operator -o jsonpath="{.status.currentCSV}"',
            "deploymentModel": 'oc get flowcollector -o jsonpath="{.items[*].spec.deploymentModel}" -n netobserv',
            "agent": 'oc get flowcollector -o jsonpath="{.items[*].spec.agent.type}"',
            "aws_s3_bucket_name": 'oc extract cm/lokistack-config -n netobserv --keys=config.yaml --confirm --to=/tmp | xargs -I {} egrep bucketnames {} | cut -d: -f 2 | xargs echo -n'
        }

        # collect data from cluster about netobserv operator and store in info dict
        self.data = self.run_commands(base_commands, self.data)

        # get flp_kind data from kafka data
        deploymentModel = self.data["deploymentModel"].lower()
        logging.debug(f"Found deployment model {deploymentModel}")
        if deploymentModel == 'kafka':
            logging.debug("Kafka is enabled - flp_kind is 'Deployment'")
            self.data["flp_kind"] = "Deployment"
        else:
            logging.debug("Kafka is disabled - flp_kind is 'DaemonSet'")
            self.data["flp_kind"] = "DaemonSet"

        # run any additional commands needed and append data to info dictionary
        agent = self.data["agent"].lower()
        logging.debug(f"Found collector agent {agent}")
        s3_bucket_name = self.data["aws_s3_bucket_name"]
        logging.debug(f"Found AWS S3 bucket name {s3_bucket_name}")
        additional_commands = {
            "sampling": f'oc get flowcollector -o jsonpath="{{.items[*].spec.agent.{agent}.sampling}}"',
            "cache_active_time": f'oc get flowcollector -o jsonpath="{{.items[*].spec.agent.{agent}.cacheActiveTimeout}}"',
            "cache_max_flows": f'oc get flowcollector -o jsonpath="{{.items[*].spec.agent.{agent}.cacheMaxFlows}}"',
            "aws_s3_bucket_usage": f'aws s3 ls --summarize --human-readable --recursive s3://{s3_bucket_name} | tail -2'
        }
        if deploymentModel == 'kafka':
            additional_commands["kafka_replicas"] = f'oc get flowcollector -o jsonpath="{{.items[*].spec.processor.kafkaConsumerReplicas}}"'
            additional_commands["kafka_brokers"] = f'oc get Kafka -o jsonpath="{{.items[*].spec.kafka.replicas}}" -n netobserv'
        else:
            self.data["kafka_replicas"] = "N/A"
            self.data["kafka_brokers"] = "N/A"
        self.data = super.run_commands(additional_commands, self.data)


class Jenkins(DataHandler):
    def __init__(self, uuid, start_time, job, build) -> None:
        super().__init__()
        self.uuid = uuid
        self.start_time = start_time
        self.job = job
        self.build = build
        self.name = 'Jenkins'
        self.url = 'https://mastern-jenkins-csb-openshift-qe.apps.ocp-c1.prod.psi.redhat.com/'
        self.server = jenkins.Jenkins(self.url)
        self.supported_workloads = ['node-density-heavy', 'router-perf', 'ingress-perf', 'cluster-density', 'cluster-density-v2']

    def collect(self):
        ''' gathers information about Jenkins env
            if build parameter data cannot be collected, only command line data will be included
        '''

        # intialize info object
        iso_timestamp = self.get_iso_timestamp(self.start_time)
        info = {
            "uuid": self.uuid,
            "metric_name": "jenkinsEnv",
            "data_type": "metadata",
            "iso_timestamp": iso_timestamp,
            "jenkins_job_name": self.job,
            "jenkins_build_num": self.build
        }

        # collect data from Jenkins server
        try:
            build_info = self.jenkins_server.get_build_info(self.job, self.build)
            logging.debug(f"Jenkins Build Info: {build_info}")
            build_actions = build_info['actions']
            build_parameters = None
            for action in build_actions:
                if action.get('_class') == 'hudson.model.ParametersAction':
                    build_parameters = action['parameters']
                    break
            if build_parameters is None:
                raise Exception("No build parameters could be found.")
            for param in build_parameters:
                del param['_class']
                if param.get('name') == 'VARIABLE':
                    info['variable'] = int(param.get('value'))
                if param.get('name') == 'WORKLOAD':
                    info['workload'] = str(param.get('value'))
            # if workload is not explicitly set in Jenkins such as with router-perf, take it from the job name 
            if info.get('workload') is None:
                info['workload'] = self.job.split('/')[-1]
            # check if a valid workload has been parsed
            if info['workload'] not in self.supported_workloads:
                raise Exception(f"{info['workload']} is not in the list of supported workloads: {self.supported_workloads}")

        except Exception as e:
            logging.error(f"Failed to collect Jenkins build parameter info: {e}")

        # store all collected data in object
        self.data = info


class Prometheus(DataHandler):
    def __init__(self, token, url, queries, start_time, end_time, step) -> None:
        super().__init__()
        self.name = 'Prometheus'
        self.token = token,
        self.url = url
        self.queries = queries
        self.start_time = start_time
        self.end_time = end_time
        self.step = step
        

    def collect(self):
        ''' TODO
        '''

        for entry in self.queries:
            metric_name = entry['metricName']
            query = entry['query']
            raw_data = self.run_query(query)
            clean_data = self.process_query(metric_name, query, raw_data)
            self.data.extend(clean_data)

    def process_query(self, metric_name, query, raw_data):
        ''' takes in a Prometheus metric name, query and its successful execution result
            reformats data into format desired for Elasticsearch upload
            returns dict object with post-processed data
        '''

        # intialize post-processing data object
        clean_data = []

        # capture metadata and value artifacts from prometheus return object
        try:
            for result in raw_data["data"]["result"]:
                for data_point in result["values"]:
                    metadata = result["metric"]
                    metadata["query"] = query
                    iso_timestamp = self.get_iso_timestamp(data_point[0])
                    clean_data.append(
                        {
                            "uuid": self.uuid,
                            "metric_name": metric_name,
                            "data_type": "datapoint",
                            "iso_timestamp": iso_timestamp,
                            "unix_timestamp": data_point[0],
                            "value": float(data_point[1]),
                            "metadata": metadata
                        }
                    )

        except Exception as e:
            timestamp = datetime.datetime.now().strftime('%Y-%m-%d_%H-%M-%S')
            logging.error(f"Error cleaning {metric_name} data from query {query}: {e}")
            logging.error(f"raw_data: {raw_data}")
            logging.error(f"Dumping partially-cleaned data to {DATA_DIR}/partial_data_{timestamp}.json")
            self.dump_data_locally(timestamp, partial=True)
            sys.exit(1)

        # return cleaned data
        return clean_data

    def run_query(self, query):
        ''' takes in a Prometheus query
            executes a range query based on global constants
            returns the JSON data delivered by Prometheus or an exception if the query fails
        '''

        # construct request
        headers = {"Authorization": f"Bearer {self.token}"}
        endpoint = f"{self.thanos_url}/api/v1/query_range"
        params = {
            'query': query,
            'start': self.start_time,
            'end': self.end_time,
            'step': self.step
        }

        # make request and return data
        data = requests.get(endpoint, headers=headers, params=params, verify=False)
        if data.status_code != 200:
            raise Exception(f"Query to fetch Prometheus data failed: {data.reason}") 
        return data.json()


class Baseline(DataHandler):
    def __init__(self) -> None:
        super().__init__()
        self.name = 'Baseline'

    def fetch(self, workload):
        ''' fetches baseline data for a given workload from Elasticsearch
        '''

        # create Elasticsearch object
        es = Elasticsearch(
            [f'https://{self.es_username}:{self.es_password}@{self.es_url}:443'],
            timeout=30,
            max_retries=10,
            retry_on_timeout=True
        )

        # fetch most recent baseline for given workload
        baselineRes = es.search(
            index='prod-netobserv-baselines',
            body={
                "query": {
                    "match": {
                        "workload.keyword": workload
                    }
                },
                "sort": [
                    {
                        "iso_timestamp": {
                            "order": "desc"
                        }
                    }
                ],
                "size": 1
            }
        )
        logging.debug(f"Response back from baseline fetch attempt was {baselineRes}")

        # ensure exactly 1 result was captured, exit if not
        baselineResNumHits = len(baselineRes['hits']['hits'])
        if baselineResNumHits != 1:
            logging.error(f"Got {baselineResNumHits} hits - should be '1'")
            sys.exit(1)

        # parse out uuid from response object and log
        baseline_uuid = baselineRes['hits']['hits'][0]['_source']['uuid']
        logging.info(f"Got {baseline_uuid} for most recent baseline for workload {workload}")

        # ensure data directory exists (create if not)
        pathlib.Path(DATA_DIR).mkdir(parents=True, exist_ok=True)

        # dump baseline to JSON so it can be consumed by other actors such as Jenkins
        with open(DATA_DIR + f'/baseline.json', 'w') as baseline_file:
            json.dump({'BASELINE_UUID':baseline_uuid}, baseline_file)

        # close Elasticsearch object and return
        es.close()
        return None


    def upload(self, uuid):
        ''' uploads baseline data for a given UUID from Elasticsearch
        '''

        # create Elasticsearch object
        es = Elasticsearch(
            [f'https://{self.es_username}:{self.es_password}@{self.es_url}:443'],
            timeout=30,
            max_retries=10,
            retry_on_timeout=True
        )

        # get netobserv release info from Elasticsearch based off UUID
        releaseRes = es.search(
            index='prod-netobserv-operator-metadata',
            body={
                "query": {
                    "match": {
                        "uuid.keyword": uuid
                    }
                }
            }
        )
        logging.debug(f"Response back from release fetch attempt was {releaseRes}")

        # ensure exactly 1 result was captured, exit if not
        releaseResNumHits = len(releaseRes['hits']['hits'])
        if releaseResNumHits != 1:
            logging.error(f"Got {releaseResNumHits} hits - should be '1'")
            sys.exit(1)

        # parse out release info from response object and log
        release = releaseRes['hits']['hits'][0]['_source']['release']
        logging.info(f"Got {release} for release from test results for UUID {uuid}")

        # get workload info from Elasticsearch based off UUID
        workloadRes = es.search(
            index='prod-netobserv-jenkins-metadata',
            body={
                "query": {
                    "match": {
                        "uuid.keyword": uuid
                    }
                }
            }
        )
        logging.debug(f"Response back from workload fetch attempt was {workloadRes}")

        # ensure exactly 1 result was captured, exit if not
        workloadResNumHits = len(workloadRes['hits']['hits'])
        if workloadResNumHits != 1:
            logging.error(f"Got {workloadResNumHits} hits - should be '1'")
            sys.exit(1)

        # parse out workload info from response object and log
        # if workload is not explicitly set in jenkins data such as with legacy router-perf, take it from the job name 
        try:
            workload = workloadRes['hits']['hits'][0]['_source']['workload']
        except KeyError:
            job_name = workloadRes['hits']['hits'][0]['_source']['jenkins_job_name']
            workload = job_name.split('/')[-1]
        logging.info(f"Got {workload} for workload from test results for UUID {uuid}")

        # assemble baseline document
        iso_timestamp = self.get_iso_timestamp(time.time())
        document = {
            "uuid": uuid,
            "data_type": "baseline",
            "iso_timestamp": iso_timestamp,
            "workload": workload,
            "release": release
        }

        # upload new baseline to Elasticsearch
        logging.debug(f"Uploading baseline document '{document}' to index 'prod-netobserv-baselines' in Elasticsearch")
        baselineUploadRes = es.index(
            index='prod-netobserv-baselines',
            body=document
        )
        logging.debug(f"Response back from upload attempt was {baselineUploadRes}")

        # close Elasticsearch object and return
        es.close()
        return None


class LocalUpload(DataHandler):
    def __init__(self) -> None:
        super().__init__()
        self.name = 'LocalUpload'

    def read(self, upload_file_path):
        with open(upload_file_path) as json_file:
            self.data = json.load(json_file)


def main(NetobservHandler, JenkinsHandler, PrometheusHandler, dump):

    # get netobserv env data
    NetobservHandler.collect()

    # get jenkins env data
    JenkinsHandler.collect()

    # get prometheus data
    PrometheusHandler.collect()

    # log success if no issues
    logging.info(f"Data captured successfully")

    # either dump data locally or upload it to Elasticsearch
    timestamp = datetime.datetime.now().strftime('%Y-%m-%d_%H-%M-%S')
    num_failures = 0
    if dump:
        NetobservHandler.dump_data_locally(timestamp)
        JenkinsHandler.dump_data_locally(timestamp)
        PrometheusHandler.dump_data_locally(timestamp)
        logging.info(f"Data written to {DATA_DIR}")
    else:
        # upload netobserv data or dump if failure
        try:
            netobserv_elapsed_time = NetobservHandler.upload_data_to_elasticsearch()
            logging.info(f"Netobserv data upload to Elasticsearch completed in {netobserv_elapsed_time} seconds")
        except Exception as e:
            logging.error(f"Error uploading to Elasticsearch server: {e}\nA local dump to {DATA_DIR} will be done instead")
            NetobservHandler.dump_data_locally(timestamp)
            num_failures += 1

        # upload jenkins data or dump if failure
        try:        
            jenkins_elapsed_time = JenkinsHandler.upload_data_to_elasticsearch()
            logging.info(f"Jenkins data upload to Elasticsearch completed in {jenkins_elapsed_time} seconds")
        except Exception as e:
            logging.error(f"Error uploading to Elasticsearch server: {e}\nA local dump to {DATA_DIR} will be done instead")
            JenkinsHandler.dump_data_locally(timestamp)
            num_failures += 1

        # upload prometheus data or dump if failure
        try:
            prometheus_elapsed_time = PrometheusHandler.upload_data_to_elasticsearch()
            logging.info(f"Prometheus data upload to Elasticsearch completed in {prometheus_elapsed_time} seconds")
        except Exception as e:
            logging.error(f"Error uploading to Elasticsearch server: {e}\nA local dump to {DATA_DIR} will be done instead")
            PrometheusHandler.dump_data_locally(timestamp)
            num_failures += 1   

    # exit 0 if no issues, exit 2 otherwise 
    #   using exit code of 2 here so that Jenkins pipeline can uniquely identify this error
    if num_failures == 0:
        logging.info(f"3/3 Elasticsearch uploads succeeded")
        sys.exit(0)
    else:
        logging.error(f"{num_failures}/3 Elasticsearch uploads failed")
        sys.exit(2)


if __name__ == '__main__':

    # initialize argument parser and mode subparsers
    parser = argparse.ArgumentParser(
        description='Network Observability Prometheus and Elasticsearch tool (NOPE)'
    )
    subparsers = parser.add_subparsers(
        dest='mode',
        help='Additional Run Modes'
    )

    # set logging flags
    parser.add_argument("--debug", default=False, action='store_true', help='Flag for additional debug messaging')

    # set standard mode flags
    standard = parser.add_argument_group("Standard Mode", "Connect to an OCP cluster and gather data via Prometheus queries")
    standard.add_argument("--yaml-file", type=str, default='netobserv_prometheus_queries.yaml', help='YAML file from which to source Prometheus queries - defaults to "netobserv_prometheus_queries.yaml"')
    standard.add_argument("--starttime", type=str, help='Start time for range query')
    standard.add_argument("--endtime", type=str, help='End time for range query')
    standard.add_argument("--step", type=str, default='60', help='Step time for range query')
    standard.add_argument("--jenkins-job", type=str, help='Jenkins job name to associate with run')
    standard.add_argument("--jenkins-build", type=str, help='Jenkins build number to associate with run')
    standard.add_argument("--uuid", type=str, help='UUID to associate with run - if none is provided one will be generated')
    standard.add_argument("--dump", default=False, action='store_true', help='Flag to dump data locally instead of uploading it to Elasticsearch')
    standard.add_argument("--jira", type=str, help='Jira ticket to associate with run - should be in the form of "NETOBSERV-123"')

    # set upload mode flags
    upload = subparsers.add_parser(
        "upload",
        description="Directly upload data from a previously generated JSON file to Elasticsearch",
        help="Directly upload data from a previously generated JSON file to Elasticsearch"
    )
    upload.add_argument("--file", type=str, required=True, help='JSON file to upload to Elasticsearch. Must be in the "data" directory.')

    # set baseline mode flags
    baseline = subparsers.add_parser(
        "baseline",
        description="Fetch an existing baseline from Elasticsearch or Upload a new baseline",
        help="Fetch an existing baseline from Elasticsearch or Upload a new baseline"
    )
    baseline_group = baseline.add_mutually_exclusive_group(required=True)
    baseline_group.add_argument("--fetch", type=str, help='Fetch the most recent baseline for a given workload')
    baseline_group.add_argument("--upload", type=str, help='Upload a new baseline for a given UUID')

    # parse arguments
    args = parser.parse_args()

    # set logging config
    if args.debug:
        logging.basicConfig(level=logging.DEBUG)
        coloredlogs.install(level='DEBUG', isatty=True)
    else:
        logging.basicConfig(level=logging.INFO)
        coloredlogs.install(level='INFO', isatty=True)

    # if running in upload mode - immediately load JSON, upload, and exit
    if args.mode == 'upload':
        upload_file_path = DATA_DIR + '/' + args.file
        logging.info(f"Running NOPE tool in Upload mode - data from {upload_file_path} will be uploaded to Elasticsearch")
        LocalUploadHandler = LocalUpload()

        # read data json file
        LocalUploadHandler.read(upload_file_path)

        # upload data to elasticsearch
        try:
            elapsed_time = LocalUploadHandler.upload_data_to_elasticsearch()
            logging.info(f"Elasticsearch upload completed in {elapsed_time} seconds")
            sys.exit(0)
        except Exception as e:
            logging.error(f"Error uploading to Elasticsearch server: {e}")
            sys.exit(1)

    # if running in baseline mode - perform action based on argument and exit
    if args.mode == 'baseline':
        BaselineHandler = Baseline()
        if args.fetch is not None:
            BaselineHandler.fetch(args.fetch)
            sys.exit(0)
        else:
            BaselineHandler.upload(args.upload)
            sys.exit(0)

    # sanity check that kubeconfig is set
    result = subprocess.run(['oc', 'whoami'], capture_output=True, text=True)
    if result.returncode != 0:
        logging.error("Could not connect to cluster - ensure all the Prerequisite steps in the README were followed")
        sys.exit(1)

    # log prometheus range query constants
    if args.starttime == '' or args.endtime == '':
        logging.error("START_TIME and END_TIME are needed to proceed")
        sys.exit(1)
    else:
        logging.info("Parsed Start Time: " + datetime.datetime.utcfromtimestamp(int(args.starttime)).strftime('%I:%M%p%Z UTC on %m/%d/%Y'))
        logging.info("Parsed End Time:   " + datetime.datetime.utcfromtimestamp(int(args.endtime)).strftime('%I:%M%p%Z UTC on %m/%d/%Y'))
        logging.info("Step is:           " + args.step)

    # determine UUID and Jira if applicable
    workload_uuid = args.UUID()
    if workload_uuid is None:
        workload_uuid = str(uuid.uuid4())
    logging.info(f"UUID: {workload_uuid}")
    jira = args.jira
    if jira is None:
        jira = "N/A"
    else:
        logging.info(f"Associating run with Jira ticket {jira}")

    # create NetobservHandler object
    NetobservHandler = (
        args.workload_uuid,
        args.starttime,
        args.jira
    )

    # get YAML file with queries and set queries constant with data from YAML file
    logging.info(f"YAML_FILE: {args.yaml_file}")
    try:
        with open(SCRIPT_DIR + '/queries/' + args.yaml_file, 'r') as yaml_file:
            prometheus_queries = yaml.safe_load(yaml_file)
    except Exception as e:
        logging.error(f'Failed to read YAML file {args.yaml_file}: {e}')
        sys.exit(1)

    # get thanos URL from cluster
    raw_thanos_url = subprocess.run(['oc', 'get', 'route', 'thanos-querier', '-n', 'openshift-monitoring', '-o', 'jsonpath="{.spec.host}"'], capture_output=True, text=True).stdout
    thanos_url = "https://" + raw_thanos_url[1:-1]
    logging.info(f"THANOS_URL: {thanos_url}")

    # get token from cluster
    prometheus_token = subprocess.run(['oc', 'create', 'token', 'prometheus-k8s', '-n', 'openshift-monitoring'], capture_output=True, text=True).stdout
    # try deprecated method in case first attempt fails
    if prometheus_token == '':
        prometheus_token = subprocess.run(['oc', 'sa', 'new-token', 'prometheus-k8s', '-n', 'openshift-monitoring'], capture_output=True, text=True).stdout

    # log token or exit if no token could be found
    if prometheus_token == '':
        logging.error("No token could be found - ensure all the Prerequisite steps in the README were followed")
        sys.exit(1)
    logging.info(f"TOKEN: {prometheus_token}")

    # check if Jenkins arguments are valid and if so create handler
    if all(v is None for v in [args.jenkins_job, args.jenkins_build]):
        logging.info("No Jenkins parameters provided - no Jenkins data will be uploaded (Crtl-C to cancel)")
        JenkinsHandler = None
    if any(v is None for v in [args.jenkins_job, args.jenkins_build]):
        logging.error("JENKINS_JOB and JENKINS_BUILD must all be used together or not at all")
        sys.exit(1)
    else:
        logging.info(f"Associating run with Jenkins job {args.jenkins_job} build number {args.jenkins_build}")
        try:
            JenkinsHandler = Jenkins(
                workload_uuid,
                args.starttime,
                args.jenkins_job,
                int(args.jenkins_build),
            )
        except Exception as e:
            logging.error("Error connecting to Jenkins server: ", e)
            sys.exit(1)

    # create PrometheusHandler object
    PrometheusHandler = Prometheus(
        prometheus_token,
        thanos_url,
        prometheus_queries,
        args.starttime,
        args.endtime,
        args.step
    )

    # determine if data will be dumped locally or uploaded to Elasticsearch
    if args.dump:
        logging.info(f"Data will be dumped locally to {DATA_DIR}")
        dump = True
    else:
        if (os.getenv('ES_USERNAME') is None) or (os.getenv('ES_PASSWORD') is None):
            logging.error("Credentials need to be set to upload data to Elasticsearch")
            sys.exit(1)
        logging.info(f"Data will be uploaded to Elasticsearch")
        dump = False

    # begin main program execution
    main(NetobservHandler, JenkinsHandler, PrometheusHandler, dump)
