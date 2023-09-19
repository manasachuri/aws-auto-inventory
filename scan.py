# -*- coding: utf-8 -*-
# Required modules
import argparse
import boto3
import botocore
import concurrent.futures
import datetime
import json
import logging
import os
import time
import traceback
from datetime import datetime
from botocore.config import Config
import requests

# Define the timestamp as a string, which will be the same throughout the execution of the script.
timestamp = datetime.now().isoformat(timespec="minutes")


def get_json_from_url(url):
    """Fetch JSON from a URL."""
    try:
        response = requests.get(url)
        response.raise_for_status()  # Raises a HTTPError if the status is 4xx, 5xx
    except requests.exceptions.RequestException as e:
        print(f"Failed to fetch JSON from {url}: {e}")
        return None

    try:
        return response.json()
    except ValueError as e:
        print(f"Failed to parse JSON from {url}: {e}")
        return None


class DateTimeEncoder(json.JSONEncoder):
    """Custom JSONEncoder that supports encoding datetime objects."""

    def default(self, o):
        if isinstance(o, datetime):
            return o.isoformat()
        return super().default(o)


def setup_logging(log_dir, log_level):
    """Set up the logging system."""
    os.makedirs(log_dir, exist_ok=True)
    log_filename = f"aws_resources_{timestamp}.log"
    log_file = os.path.join(log_dir, log_filename)

    # Configure the logger
    logger = logging.getLogger(__name__)
    logger.setLevel(log_level)
    handler = logging.FileHandler(log_file)
    handler.setLevel(log_level)
    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logging.basicConfig(level=log_level)
    return logging.getLogger(__name__)


def api_call_with_retry(session, client, function_name, parameters, max_retries, retry_delay):
    """
    Make an API call with exponential backoff.

    This function will make an API call with retries. It will exponentially back off
    with a delay of `retry_delay * 2^attempt` for transient errors.
    """

    def api_call():
        for attempt in range(max_retries):
            try:
                if function_name == 'list_buckets':
                    function_to_call = getattr(client, function_name)
                    if parameters:
                        return function_to_call(**parameters)
                    else:
                        return function_to_call()
            #Pagination for Firehose list_delivery_streams
                elif function_name == 'list_delivery_streams':
                    delivery_streams = []
                    exclusive_start_delivery_stream_name = None
                    while True:
                        if exclusive_start_delivery_stream_name == None:
                            response = client.list_delivery_streams(
                                Limit=100,  # Adjust the limit as per your requirements
                            )
                        else:
                            response = client.list_delivery_streams(
                                Limit=100,  # Adjust the limit as per your requirements
                                ExclusiveStartDeliveryStreamName=exclusive_start_delivery_stream_name
                            )
                        delivery_streams.extend(response['DeliveryStreamNames'])
                        if not response.get('HasMoreDeliveryStreams'):
                            break
                        
                        exclusive_start_delivery_stream_name = response['DeliveryStreamNames'][-1]
                    return delivery_streams
                paginator = client.get_paginator(function_name)
                function_to_call = getattr(paginator, 'paginate')
                if parameters:
                    if parameters == 'OwnerIds':
                        account_id = session.client('sts').get_caller_identity().get('Account')
                        new_parameters = {'OwnerIds': [account_id]}
                        return function_to_call(**new_parameters)
                    return function_to_call(**parameters)
                else:
                    return function_to_call()
            except botocore.exceptions.ClientError as error:
                error_code = error.response["Error"]["Code"]
                if error_code == "Throttling":
                    if attempt < (max_retries - 1):  # no delay on last attempt
                        time.sleep(retry_delay**attempt)
                    continue
                elif error_code == "RequestLimitExceeded":
                    time.sleep(retry_delay**attempt)
                    continue
                else:
                    raise
            except botocore.exceptions.BotoCoreError:
                if attempt < (max_retries - 1):  # no delay on last attempt
                    time.sleep(retry_delay**attempt)
                continue
        return None

    return api_call


def _get_service_data(session, region_name, service, log, max_retries, retry_delay):
    """
    Get data for a specific AWS service in a region.

    Arguments:
    session -- The boto3 Session.
    region_name -- The AWS region to process.
    service -- The AWS service to scan.
    log -- The logger object.
    max_retries -- The maximum number of retries for each service.
    retry_delay -- The delay before each retry.

    Returns:
    service_data -- The service data.
    """

    function = service["function"]
    result_key = service.get("result_key", None)
    parameters = service.get("parameters", None)

    log.info(
        "Getting data on service %s with function %s in region %s. Also, ResultKey and Parameters are as follows: %s %s",
        service["service"],
        function,
        region_name,
        result_key,
        parameters,
    )

    try:
        client = session.client(service["service"], region_name=region_name)
        if not hasattr(client, function):
            log.error(
                "Function %s does not exist for service %s in region %s",
                function,
                service["service"],
                region_name,
            )
            return None
        api_call = api_call_with_retry(
            session, client, function, parameters, max_retries, retry_delay
        )
        if result_key:
            response = api_call().get(result_key)
        else:
            response = api_call()
            if isinstance(response, dict):
                response.pop("ResponseMetadata", None)
    except Exception as exception:
        log.error(
            "Error while processing %s, %s.\n%s: %s",
            service["service"],
            region_name,
            type(exception).__name__,
            exception,
        )
        log.error(traceback.format_exc())
        return None

    log.info("Finished: AWS Get Service Data")
    log.debug(
        "Result for %s, function %s, region %s: %s",
        service["service"],
        function,
        region_name,
        response,
    )
    return {"region": region_name, "service": service["service"], "result": response}


def process_region(
    region, services, session, log, max_retries, retry_delay, concurrent_services
):
    """
    Processes a single AWS region.

    Arguments:
    region -- The AWS region to process.
    services -- The AWS services to scan.
    session -- The boto3 Session.
    log -- The logger object.
    max_retries -- The maximum number of retries for each service.
    retry_delay -- The delay before each retry.
    concurrent_services -- The number of services to process concurrently for each region.

    Returns:
    region_results -- The scan results for the region.
    """

    log.info("Started processing for region: %s", region)

    region_results = []
    with concurrent.futures.ThreadPoolExecutor(
        max_workers=concurrent_services
    ) as executor:
        future_to_service = {
            executor.submit(
                _get_service_data,
                session,
                region,
                service,
                log,
                max_retries,
                retry_delay,
            ): service
            for service in services
        }
        for future in concurrent.futures.as_completed(future_to_service):
            service = future_to_service[future]
            try:
                result = future.result()
                if result is not None and result["result"]:
                    region_results.append(result)
                    log.info("Successfully processed service: %s - %s", service["service"], service["function"],)
                else:
                    log.info("No data found for service: %s - %s", service["service"], service["function"],)
            except Exception as exc:
                log.error("%r generated an exception: %s" % (service["service"], exc))
                log.error(traceback.format_exc())

    log.info("Finished processing for region: %s", region)
    return region_results


def display_time(seconds):
    hours = seconds // 3600
    minutes = (seconds % 3600) // 60
    seconds = seconds % 60
    return f"{int(hours)}h:{int(minutes)}m:{int(seconds)}s"


def check_aws_credentials(session):
    """Check AWS credentials by calling the STS GetCallerIdentity operation."""
    try:
        sts = session.client("sts")
        identity = sts.get_caller_identity()
        print(f"Authenticated as: {identity['Arn']}")
    except botocore.exceptions.BotoCoreError as error:
        print(f"Error verifying AWS credentials: {error}")
        return False

    return True

"""
Descrive DynamoDB Tables
"""
def dynamodb_process_table(table, dynamodb_client, session):
    response = dynamodb_client.describe_table(TableName=table)
    new_table = {}
    new_table['AccountID'] = session.client('sts').get_caller_identity().get('Account')
    new_table['Region'] = session.region_name
    new_table['Arn'] = response['Table']['TableArn']
    new_table['Tags'] = dynamodb_client.list_tags_of_resource(ResourceArn=new_table['Arn'])['Tags']
    return new_table

def get_dynamodb_tables(result, session, region, log):
    new_result = {'dynamodb': []}

    def dynamodb_process_page(page, dynamodb_client, session):
        tables = page['TableNames']
        with concurrent.futures.ThreadPoolExecutor() as executor:
            futures = []
            for table in tables:
                future = executor.submit(dynamodb_process_table, table, dynamodb_client, session)
                futures.append(future)
            for future in concurrent.futures.as_completed(futures):
                new_table = future.result()
                new_result['dynamodb'].append(new_table)

    max_connections = 100
    custom_config = Config(max_pool_connections=max_connections,    retries = {'max_attempts': 10, 'mode': 'standard'})
    dynamodb_client = session.client('dynamodb', region_name=region, config=custom_config)
    page_iterator = result['dynamodb'][0]
    for page in page_iterator:
        dynamodb_process_page(page, dynamodb_client, session)
    
    return new_result

"""
Describe S3 Buckets
"""

def s3_process_bucket(bucket, s3_client, session, log):
    new_bucket = {}
    new_bucket['AccountID'] = session.client('sts').get_caller_identity().get('Account')
    
    # Handle errors with exponential backoff retry for get_bucket_location
    max_retries = 5
    base_backoff_time = 1  # Initial wait time in seconds
    for retry in range(max_retries + 1):
        try:
            response = s3_client.get_bucket_location(Bucket=bucket['Name'])
            location = response.get('LocationConstraint', 'us-east-1')  # Default to 'us-east-1' if not present
            new_bucket['Location'] = location
            break  # If successful, break out of retry loop
        except botocore.exceptions.ClientError as error:
            if retry < max_retries:
                error_code = error.response.get('Error', {}).get('Code')
                if error_code == 'Throttling' or error_code == 'RequestLimitExceeded':
                    print(f"Throttling encountered for get_bucket_location, retrying ({retry + 1}/{max_retries + 1})")
                else:
                    print(f"Error encountered for get_bucket_location, retrying ({retry + 1}/{max_retries + 1}): {error_code}")
                backoff_time = base_backoff_time * (2 ** retry)
                print(f"Waiting for {backoff_time} seconds before retrying...")
                time.sleep(backoff_time)
            else:
                print("Max retries reached for get_bucket_location")
                log.info(f"Error getting location for bucket {bucket['Name']}: {error}")
                new_bucket['Location'] = 'us-east-1'  # Default to 'us-east-1' if error
            continue

    # Handle errors with exponential backoff retry for get_bucket_tagging
    max_retries = 5
    base_backoff_time = 1  # Initial wait time in seconds
    for retry in range(max_retries + 1):
        try:
            tags_response = s3_client.get_bucket_tagging(Bucket=bucket['Name'])
            new_bucket['Tags'] = tags_response['TagSet']
            break  # If successful, break out of retry loop
        except botocore.exceptions.ClientError as error:
            if retry < max_retries:
                error_code = error.response.get('Error', {}).get('Code')
                if error_code == 'Throttling' or error_code == 'RequestLimitExceeded':
                    print(f"Throttling encountered for get_bucket_tagging, retrying ({retry + 1}/{max_retries + 1})")
                elif error_code == 'NoSuchTagSet':
                    print(f"No tags found for bucket {bucket['Name']}")
                    new_bucket['Tags'] = []
                    break  # If successful, break out of retry loop
                elif error_code == 'AccessDenied':
                    print(f"Access Denied for bucket {bucket['Name']}")
                    new_bucket['Tags'] = []
                    break  # If successful, break out of retry loop
                else:
                    print(f"Error encountered for get_bucket_tagging, retrying ({retry + 1}/{max_retries + 1}): {error_code}")
                backoff_time = base_backoff_time * (2 ** retry)
                print(f"Waiting for {backoff_time} seconds before retrying...")
                time.sleep(backoff_time)
            else:
                print("Max retries reached for get_bucket_tagging")
                log.info(f"Error getting tags for bucket {bucket['Name']}: {error}")
                new_bucket['Tags'] = []
            continue

    new_bucket['Arn'] = 'arn:aws:s3:::' + bucket['Name']
    return new_bucket

def get_s3_buckets(result, session, region, log):
    new_result = {'s3': []}
    
    def s3_process_buckets(buckets, s3_client, session, log):
        with concurrent.futures.ThreadPoolExecutor() as executor:
            futures = []
            for bucket in buckets:
                future = executor.submit(s3_process_bucket, bucket, s3_client, session, log)
                futures.append(future)
            for future in concurrent.futures.as_completed(futures):
                new_bucket = future.result()
                new_result['s3'].append(new_bucket)

    max_connections = 100
    custom_config = Config(max_pool_connections=max_connections)
    if region == 'us-east-1':
        s3_client = session.client('s3', region_name=region, config=custom_config)
        buckets = result['s3'][0]['Buckets']
        s3_process_buckets(buckets, s3_client, session, log)
    
    return new_result

"""
Describe Lambda Functions
"""
    
def lambda_process_function(function, lambda_client, session):
    response = lambda_client.get_function_configuration(FunctionName=function['FunctionName'])
    new_function = {}
    new_function['AccountID'] = session.client('sts').get_caller_identity().get('Account')
    new_function['Region'] = session.region_name
    new_function['Arn'] = response['FunctionArn']
    new_function['Tags'] = lambda_client.list_tags(Resource=new_function['Arn'])['Tags']
    return new_function

def get_lambda_functions(result, session, region, log):
    new_result = {'lambda': []}
    
    def lambda_process_page(page, lambda_client, session):
        functions = page['Functions']
        with concurrent.futures.ThreadPoolExecutor() as executor:
            futures = []
            for function in functions:
                future = executor.submit(lambda_process_function, function, lambda_client, session)
                futures.append(future)
            for future in concurrent.futures.as_completed(futures):
                new_function = future.result()
                new_result['lambda'].append(new_function)
    max_connections = 100
    custom_config = Config(max_pool_connections=max_connections, retries = {'max_attempts': 10, 'mode': 'standard'})
    lambda_client = session.client('lambda', region_name=region, config=custom_config)
    page_iterator = result['lambda'][0]
    for page in page_iterator:
        lambda_process_page(page, lambda_client, session)
    
    return new_result

"""
Describe AppSync graphqlApi
"""
def appsync_process_api(api, appsync_client, session):
    new_api = {}
    new_api['AccountID'] = session.client('sts').get_caller_identity().get('Account')
    new_api['Region'] = session.region_name
    new_api['Arn'] = api['arn']
    new_api['Tags'] = appsync_client.list_tags_for_resource(resourceArn=new_api['Arn'])['tags']
    return new_api

def get_appsync_graphql_api(result, session, region, log):
    new_result = {'appsync': []}
    def appsync_process_page(page, appsync_client, session):
        apis = page['graphqlApis']
        with concurrent.futures.ThreadPoolExecutor() as executor:
            futures = []
            for api in apis:
                future = executor.submit(appsync_process_api, api, appsync_client, session)
                futures.append(future)
            for future in concurrent.futures.as_completed(futures):
                new_api = future.result()
                new_result['appsync'].append(new_api)
    max_connections = 100
    custom_config = Config(max_pool_connections=max_connections, retries = {'max_attempts': 10, 'mode': 'standard'})
    appsync_client = session.client('appsync', region_name=region, config=custom_config)
    page_iterator = result['appsync'][0]
    for page in page_iterator:
        appsync_process_page(page, appsync_client, session)
    
    return new_result

"""
Describe DirectConnect Connections
"""
def directconnect_process_connection(connection, directconnect_client, session):
    new_connection = {}
    new_connection['AccountID'] = session.client('sts').get_caller_identity().get('Account')
    new_connection['Region'] = session.region_name
    new_connection['ConnectionID'] = connection['connectionId']
    new_connection['Tags'] = directconnect_client.list_tags_for_resource(resourceArn=new_connection['ConnectionID'])['tags']
    return new_connection

def get_directconnect_connections(result, session, region, log):
    new_result = {'directconnect': []}
    def directconnect_process_page(page, directconnect_client, session):
        connections = page['connections']
        with concurrent.futures.ThreadPoolExecutor() as executor:
            futures = []
            for connection in connections:
                future = executor.submit(directconnect_process_connection, connection, directconnect_client, session)
                futures.append(future)
            for future in concurrent.futures.as_completed(futures):
                new_connection = future.result()
                new_result['directconnect'].append(new_connection)
    max_connections = 100
    custom_config = Config(max_pool_connections=max_connections, retries = {'max_attempts': 10, 'mode': 'standard'})
    directconnect_client = session.client('directconnect', region_name=region, config=custom_config)
    page_iterator = result['directconnect'][0]
    for page in page_iterator:
        directconnect_process_page(page, directconnect_client, session)
    
    return new_result

"""
Describe ELB Load Balancers V1
"""
def elb_process_load_balancer(load_balancer, elb_client, session):
    new_load_balancer = {}
    new_load_balancer['AccountId'] = session.client('sts').get_caller_identity()['Account']
    new_load_balancer['Region'] = session.region_name
    new_load_balancer['LoadBalancerArn'] = load_balancer['loadBalancerArn']
    new_load_balancer['Tags'] = elb_client.describe_tags(LoadBalancerNames=[new_load_balancer['LoadBalancerName']])['TagDescriptions'][0]['Tags']
    return new_load_balancer

def get_elb_load_balancers(result, session, region, log):
    new_result = {'elb': []}
    def elb_process_page(page, elb_client, session):
        load_balancers = page['loadBalancerDescriptions']
        with concurrent.futures.ThreadPoolExecutor() as executor:
            futures = []
            for load_balancer in load_balancers:
                future = executor.submit(elb_process_load_balancer, load_balancer, elb_client, session)
                futures.append(future)
            for future in concurrent.futures.as_completed(futures):
                new_load_balancer = future.result()
                new_result['elb'].append(new_load_balancer)
    max_connections = 100
    custom_config = Config(max_pool_connections=max_connections, retries = {'max_attempts': 10, 'mode': 'standard'})
    elb_client = session.client('elb', region_name=region, config=custom_config)
    page_iterator = result['elb'][0]
    for page in page_iterator:
        elb_process_page(page, elb_client, session)

    return new_result

"""
Describe ELB Load Balancers V2
"""
def elbv2_process_load_balancer(load_balancer, elbv2_client, session):
    new_load_balancer = {}
    new_load_balancer['AccountId'] = session.client('sts').get_caller_identity()['Account']
    new_load_balancer['Region'] = session.region_name
    new_load_balancer['LoadBalancerArn'] = load_balancer['LoadBalancerArn']
    new_load_balancer['Tags'] = elbv2_client.describe_tags(ResourceArns=[new_load_balancer['LoadBalancerName']])['TagDescriptions'][0]['Tags']
    return new_load_balancer

def get_elbv2_load_balancers(result, session, region, log):
    new_result = {'elbv2': []}
    def elbv2_process_page(page, elbv2_client, session):
        load_balancers = page['LoadBalancers']
        with concurrent.futures.ThreadPoolExecutor() as executor:
            futures = []
            for load_balancer in load_balancers:
                future = executor.submit(elbv2_process_load_balancer, load_balancer, elbv2_client, session)
                futures.append(future)
            for future in concurrent.futures.as_completed(futures):
                new_load_balancer = future.result()
                new_result['elbv2'].append(new_load_balancer)
    max_connections = 100
    custom_config = Config(max_pool_connections=max_connections, retries = {'max_attempts': 10, 'mode': 'standard'})
    elbv2_client = session.client('elbv2', region_name=region, config=custom_config)
    page_iterator = result['elbv2'][0]
    for page in page_iterator:
        elbv2_process_page(page, elbv2_client, session)

    return new_result

"""
Describe Event Rules
"""
def events_process_rule(rule, events_client, session, log):
    new_rule = {}
    new_rule['AccountId'] = session.client('sts').get_caller_identity()['Account']
    new_rule['Region'] = session.region_name
    new_rule['Arn'] = rule['Arn']
    new_rule['Tags'] = events_client.list_tags_for_resource(ResourceARN=new_rule['Arn'])['Tags']
    return new_rule

def get_events(result, session, region, log):
    new_result = {'events': []}
    def events_process_page(page, events_client, session):
        rules = page['Rules']
        with concurrent.futures.ThreadPoolExecutor() as executor:
            futures = []
            for rule in rules:
                future = executor.submit(events_process_rule, rule, events_client, session, log)
                futures.append(future)
            for future in concurrent.futures.as_completed(futures):
                new_rule = future.result()
                new_result['events'].append(new_rule)
    max_connections = 100
    custom_config = Config(max_pool_connections=max_connections, retries = {'max_attempts': 10, 'mode': 'standard'})
    events_client = session.client('events', region_name=region, config=custom_config)
    page_iterator = result['events'][0]
    for page in page_iterator:
        events_process_page(page, events_client, session)

    return new_result

"""
Describe SQS Queues
"""
def sqs_process_queue(queue, sqs_client, session, log):
    new_queue = {}
    new_queue['AccountId'] = session.client('sts').get_caller_identity()['Account']
    new_queue['Region'] = session.region_name
    new_queue['Arn'] = sqs_client.get_queue_attributes(QueueUrl=queue, AttributeNames=['QueueArn'])['Attributes']['QueueArn']
    new_queue['Tags'] = sqs_client.list_queue_tags(QueueUrl=queue)
    return new_queue

def get_sqs_queues(result, session, region, log):
    new_result = {'sqs': []}
    def sqs_process_page(page, sqs_client, session):
        if 'QueueUrls' in page:
            queues = page['QueueUrls']
            with concurrent.futures.ThreadPoolExecutor() as executor:
                futures = []
                for queue in queues:
                    future = executor.submit(sqs_process_queue, queue, sqs_client, session, log)
                    futures.append(future)
                for future in concurrent.futures.as_completed(futures):
                    new_queue = future.result()
                    new_result['sqs'].append(new_queue)
    max_connections = 100
    custom_config = Config(max_pool_connections=max_connections, retries = {'max_attempts': 10, 'mode': 'standard'})
    sqs_client = session.client('sqs', region_name=region, config=custom_config)
    page_iterator = result['sqs'][0]
    for page in page_iterator:
        sqs_process_page(page, sqs_client, session)

    return new_result

"""
Describe Kinesis Streams
"""
def kinesis_process_stream(stream, kinesis_client, session, log):
    new_stream = {}
    new_stream['AccountId'] = session.client('sts').get_caller_identity()['Account']
    new_stream['Region'] = session.region_name
    new_stream['Arn'] = kinesis_client.describe_stream(StreamName=stream)['StreamDescription']['StreamARN']
    new_stream['Tags'] = kinesis_client.list_tags_for_stream(StreamName=stream)['Tags']
    return new_stream

def get_kinesis_streams(result, session, region, log):
    new_result = {'kinesis': []}
    def kinesis_process_page(page, kinesis_client, session):
        streams = page['StreamNames']
        with concurrent.futures.ThreadPoolExecutor() as executor:
            futures = []
            for stream in streams:
                future = executor.submit(kinesis_process_stream, stream, kinesis_client, session, log)
                futures.append(future)
            for future in concurrent.futures.as_completed(futures):
                new_stream = future.result()
                new_result['kinesis'].append(new_stream)
    max_connections = 100
    custom_config = Config(max_pool_connections=max_connections, retries = {'max_attempts': 10, 'mode': 'standard'})
    kinesis_client = session.client('kinesis', region_name=region, config=custom_config)
    page_iterator = result['kinesis'][0]
    for page in page_iterator:
        kinesis_process_page(page, kinesis_client, session)

    return new_result

"""
Describe Kenisis firehose
"""
def firehose_process_delivery_stream(stream, firehose_client, session, log):
    new_stream = {}
    new_stream['AccountId'] = session.client('sts').get_caller_identity()['Account']
    new_stream['Region'] = session.region_name
    new_stream['Arn'] = firehose_client.describe_delivery_stream(DeliveryStreamName=stream)['DeliveryStreamDescription']['DeliveryStreamARN']
    new_stream['Tags'] = firehose_client.list_tags_for_delivery_stream(DeliveryStreamName=stream)['Tags']
    return new_stream

def get_firehose_streams(result, session, region, log):
    new_result = {'firehose': []}
    def firehose_process_page(page, firehose_client, session):
        streams = page
        with concurrent.futures.ThreadPoolExecutor() as executor:
            futures = []
            for stream in streams:
                future = executor.submit(firehose_process_delivery_stream, stream, firehose_client, session, log)
                futures.append(future)
            for future in concurrent.futures.as_completed(futures):
                new_stream = future.result()
                new_result['firehose'].append(new_stream)
    max_connections = 100
    custom_config = Config(max_pool_connections=max_connections, retries = {'max_attempts': 10, 'mode': 'standard'})
    firehose_client = session.client('firehose', region_name=region, config=custom_config)
    page_iterator = result['firehose']
    for page in page_iterator:
        firehose_process_page(page, firehose_client, session)

    return new_result

"""
Describe State Machines
"""
def state_machine_process_state_machine(machine, sm_client, session, log):
    new_machine = {}
    new_machine['AccountId'] = session.client('sts').get_caller_identity()['Account']
    new_machine['Region'] = session.region_name
    new_machine['Arn'] = sm_client.describe_state_machine(stateMachineArn=machine)['stateMachineArn']
    new_machine['Tags'] = sm_client.list_tags_for_resource(resourceArn=machine)['tags']
    return new_machine

def get_state_machines(result, session, region, log):
    new_result = {'stepfunctions': []}
    def state_machine_process_page(page, sm_client, session):
        machines = page['stateMachines']
        with concurrent.futures.ThreadPoolExecutor() as executor:
            futures = []
            for machine in machines:
                future = executor.submit(state_machine_process_state_machine, machine['stateMachineArn'], sm_client, session, log)
                futures.append(future)
            for future in concurrent.futures.as_completed(futures):
                new_machine = future.result()
                new_result['stepfunctions'].append(new_machine)
    max_connections = 100
    custom_config = Config(max_pool_connections=max_connections, retries = {'max_attempts': 10, 'mode': 'standard'})
    sm_client = session.client('stepfunctions', region_name=region, config=custom_config)
    page_iterator = result['stepfunctions'][0]
    for page in page_iterator:
        state_machine_process_page(page, sm_client, session)

    return new_result

"""
Describe Connect Instances
"""
def connect_process_instance(instance, connect_client, session, log):
    new_instance = {}
    new_instance['AccountId'] = session.client('sts').get_caller_identity()['Account']
    new_instance['Region'] = session.region_name
    new_instance['Arn'] = connect_client.describe_instance(InstanceId=instance['Id'])['Instance']['Arn']
    try:
        new_instance['Tags'] = connect_client.list_tags_for_resource(resourceArn=new_instance['Arn'])['tags']
    except botocore.exceptions.ClientError as error:
        error_code = error.response.get('Error', {}).get('Code')
        print(f"Error Code: {error_code}")
        if error_code == 'BadRequestException':
            new_instance['Tags'] = []
        else:
            print(f"Error encountered : {error}")

    return new_instance

def get_connect_instances(result, session, region, log):
    new_result = {'connect': []}
    def connect_process_page(page, connect_client, session):
        instances = page['InstanceSummaryList']
        with concurrent.futures.ThreadPoolExecutor() as executor:
            futures = []
            for instance in instances:
                future = executor.submit(connect_process_instance, instance, connect_client, session, log)
                futures.append(future)
            for future in concurrent.futures.as_completed(futures):
                new_instance = future.result()
                new_result['connect'].append(new_instance)
    max_connections = 100
    custom_config = Config(max_pool_connections=max_connections, retries = {'max_attempts': 10, 'mode': 'standard'})
    connect_client = session.client('connect', region_name=region, config=custom_config)
    page_iterator = result['connect'][0]
    for page in page_iterator:
        connect_process_page(page, connect_client, session)

    return new_result

"""
Describe CloudWatch Alarms
"""
def cloudwatch_process_alarm(alarm, cw_client, session, log):
    new_alarm = {}
    new_alarm['AccountId'] = session.client('sts').get_caller_identity()['Account']
    new_alarm['Region'] = session.region_name
    new_alarm['Arn'] = alarm['AlarmArn']
    new_alarm['Tags'] = cw_client.list_tags_for_resource(ResourceARN=new_alarm['Arn'])['Tags']
    return new_alarm

def get_cloudwatch_alarms(result, session, region, log):
    new_result = {'cloudwatch': []}
    def cloudwatch_process_page(page, cw_client, session):
        alarms = page['MetricAlarms']
        with concurrent.futures.ThreadPoolExecutor() as executor:
            futures = []
            for alarm in alarms:
                future = executor.submit(cloudwatch_process_alarm, alarm, cw_client, session, log)
                futures.append(future)
            for future in concurrent.futures.as_completed(futures):
                new_alarm = future.result()
                new_result['cloudwatch'].append(new_alarm)
    max_connections = 100
    custom_config = Config(max_pool_connections=max_connections, retries = {'max_attempts': 10, 'mode': 'standard'})
    cw_client = session.client('cloudwatch', region_name=region, config=custom_config)
    page_iterator = result['cloudwatch'][0]
    for page in page_iterator:
        cloudwatch_process_page(page, cw_client, session)

    return new_result

"""
Describe CloudWatch Logs
"""
def logs_process_log_group(log_group, cwl_client, session, log):
    new_log_group = {}
    new_log_group['AccountId'] = session.client('sts').get_caller_identity()['Account']
    new_log_group['Region'] = session.region_name
    if log_group['arn'].endswith(':*'):
        new_log_group['Arn'] = log_group['arn'][:-2]
    new_log_group['Tags'] = cwl_client.list_tags_for_resource(resourceArn=new_log_group['Arn'])['tags']
    return new_log_group

def get_log_groups(result, session, region, log):
    new_result = {'logs': []}
    def logs_process_page(page, cwl_client, session):
        loggroups = page['logGroups']
        with concurrent.futures.ThreadPoolExecutor() as executor:
            futures = []
            for loggroup in loggroups:
                future = executor.submit(logs_process_log_group, loggroup, cwl_client, session, log)
                futures.append(future)
            for future in concurrent.futures.as_completed(futures):
                new_log_group = future.result()
                new_result['logs'].append(new_log_group)
    max_connections = 100
    custom_config = Config(max_pool_connections=max_connections, retries = {'max_attempts': 10, 'mode': 'standard'})
    cwl_client = session.client('logs', region_name=region, config=custom_config)
    page_iterator = result['logs'][0]
    for page in page_iterator:
        logs_process_page(page, cwl_client, session)

    return new_result

"""
Describe X-Ray Groups
"""
def xray_process_group(group, xray_client, session, log):
    new_group = {}
    new_group['AccountId'] = session.client('sts').get_caller_identity()['Account']
    new_group['Region'] = session.region_name
    new_group['Arn'] = group['GroupARN']
    try:
        new_group['Tags'] = xray_client.list_tags_for_resource(ResourceARN=new_group['Arn'])['Tags']
    except botocore.exceptions.ClientError as error:
        error_code = error.response.get('Error', {}).get('Code')
        print(f"Error Code: {error_code}")
        new_group['Tags'] = []

    return new_group

def get_xray_groups(result, session, region, log):
    new_result = {'xray': []}
    def xray_process_page(page, xray_client, session):
        groups = page['Groups']
        with concurrent.futures.ThreadPoolExecutor() as executor:
            futures = []
            for group in groups:
                future = executor.submit(xray_process_group, group, xray_client, session, log)
                futures.append(future)
            for future in concurrent.futures.as_completed(futures):
                new_group = future.result()
                new_result['xray'].append(new_group)
    max_connections = 100
    custom_config = Config(max_pool_connections=max_connections, retries = {'max_attempts': 10, 'mode': 'standard'})
    xray_client = session.client('xray', region_name=region, config=custom_config)
    page_iterator = result['xray'][0]
    for page in page_iterator:
        xray_process_page(page, xray_client, session)

    return new_result

"""
Describe Config Rules
"""
def config_process_rule(rule, config_client, session, log):
    new_rule = {}
    new_rule['AccountId'] = session.client('sts').get_caller_identity()['Account']
    new_rule['Region'] = session.region_name
    new_rule['Arn'] = rule['ConfigRuleArn']
    new_rule['Tags'] = config_client.list_tags_for_resource(ResourceArn=new_rule['Arn'])['Tags']
    return new_rule

def get_config_rules(result, session, region, log):
    new_result = {'config': []}
    def config_process_page(page, config_client, session):
        rules = page['ConfigRules']
        with concurrent.futures.ThreadPoolExecutor() as executor:
            futures = []
            for rule in rules:
                future = executor.submit(config_process_rule, rule, config_client, session, log)
                futures.append(future)
            for future in concurrent.futures.as_completed(futures):
                new_rule = future.result()
                new_result['config'].append(new_rule)
    max_connections = 100
    custom_config = Config(max_pool_connections=max_connections, retries = {'max_attempts': 10, 'mode': 'standard'})
    config_client = session.client('config', region_name=region, config=custom_config)
    page_iterator = result['config'][0]
    for page in page_iterator:
        config_process_page(page, config_client, session)

    return new_result

"""
Describe KMS Keys
"""
def kms_process_keys(keys, kms_client, session, log):
    new_resource = {}
    new_resource['AccountId'] = session.client('sts').get_caller_identity()['Account']
    new_resource['Region'] = session.region_name
    new_resource['Arn'] = f"arn:aws:kms:{new_resource['Region']}:{new_resource['AccountId']}:key/{keys['KeyId']}"
    try:
        new_resource['Tags'] = kms_client.list_resource_tags(KeyId=keys['KeyId'])['Tags']
    except botocore.exceptions.ClientError as error:
        error_code = error.response.get('Error', {}).get('Code')
        print(f"Error Code: {error_code}")
        new_resource['Tags'] = []
    return new_resource

def get_kms_keys(result, session, region, log):
    new_result = {'kms': []}
    def kms_process_page(page, kms_client, session):
        keys = page['Keys']
        with concurrent.futures.ThreadPoolExecutor() as executor:
            futures = []
            for key in keys:
                future = executor.submit(kms_process_keys, key, kms_client, session, log)
                futures.append(future)
            for future in concurrent.futures.as_completed(futures):
                new_key = future.result()
                new_result['kms'].append(new_key)
    max_connections = 100
    custom_config = Config(max_pool_connections=max_connections, retries = {'max_attempts': 10, 'mode': 'standard'})
    kms_client = session.client('kms', region_name=region, config=custom_config)
    page_iterator = result['kms'][0]
    for page in page_iterator:
        kms_process_page(page, kms_client, session)

    return new_result

"""
Describe Secrets Manager Secrets
"""
def secrets_process_secrets(secrets, secrets_client, session, log):
    new_resource = {}
    new_resource['AccountId'] = session.client('sts').get_caller_identity()['Account']
    new_resource['Region'] = session.region_name
    new_resource['Arn'] = secrets['ARN']
    if 'Tags' in secrets:
        new_resource['Tags'] = secrets['Tags']
    else:
        new_resource['Tags'] = []
    return new_resource

def get_secrets_manager_secrets(result, session, region, log):
    new_result = {'secretsmanager': []}
    def secrets_process_page(page, secrets_client, session):
        secrets = page['SecretList']
        with concurrent.futures.ThreadPoolExecutor() as executor:
            futures = []
            for secret in secrets:
                future = executor.submit(secrets_process_secrets, secret, secrets_client, session, log)
                futures.append(future)
            for future in concurrent.futures.as_completed(futures):
                new_secret = future.result()
                new_result['secretsmanager'].append(new_secret)
    max_connections = 100
    custom_config = Config(max_pool_connections=max_connections, retries = {'max_attempts': 10, 'mode': 'standard'})
    secrets_client = session.client('secretsmanager', region_name=region, config=custom_config)
    page_iterator = result['secretsmanager'][0]
    for page in page_iterator:
        secrets_process_page(page, secrets_client, session)

    return new_result

"""
Describe Service Catalog Products
"""
def servicecatalog_process_products(product, service_catalog_client, session, log):
    new_resource = {}
    new_resource['AccountId'] = session.client('sts').get_caller_identity()['Account']
    new_resource['Region'] = session.region_name
    new_resource['Arn'] = product['ProductARN']
    new_resource['TagOptions'] = []
    return new_resource

def get_servicecatalog_products(result, session, region, log):
    new_result = {'servicecatalog': []}
    def service_catalog_process_page(page, service_catalog_client, session):
        products = page['ProductViewDetails']
        with concurrent.futures.ThreadPoolExecutor() as executor:
            futures = []
            for product in products:
                future = executor.submit(servicecatalog_process_products, product, service_catalog_client, session, log)
                futures.append(future)
            for future in concurrent.futures.as_completed(futures):
                new_product = future.result()
                new_result['servicecatalog'].append(new_product)
    max_connections = 100
    custom_config = Config(max_pool_connections=max_connections, retries = {'max_attempts': 10, 'mode': 'standard'})
    service_catalog_client = session.client('servicecatalog', region_name=region, config=custom_config)
    page_iterator = result['servicecatalog'][0]
    for page in page_iterator:
        service_catalog_process_page(page, service_catalog_client, session)

    return new_result

"""
Describe Service Catalog App-Registry Applications
"""
def servicecatalog_process_applications(application, service_catalog_client, session, log):
    new_resource = {}
    new_resource['AccountId'] = session.client('sts').get_caller_identity()['Account']
    new_resource['Region'] = session.region_name
    new_resource['Arn'] = application['arn']
    new_resource['TagOptions'] = service_catalog_client.list_tags_for_resource(resourceArn=new_resource['Arn'])['tags']
    return new_resource

def get_servicecatalog_appregistry_apps(result, session, region, log):
    new_result = {'servicecatalog-appregistry': []}
    def service_catalog_appregistry_process_page(page, service_catalog_appregistry_client, session):
        applications = page['applications']
        with concurrent.futures.ThreadPoolExecutor() as executor:
            futures = []
            for application in applications:
                future = executor.submit(servicecatalog_process_applications, application, service_catalog_appregistry_client, session, log)
                futures.append(future)
            for future in concurrent.futures.as_completed(futures):
                new_application = future.result()
                new_result['servicecatalog-appregistry'].append(new_application)
    max_connections = 100
    custom_config = Config(max_pool_connections=max_connections, retries = {'max_attempts': 10, 'mode': 'standard'})
    service_catalog_appregistry_client = session.client('servicecatalog-appregistry', region_name=region, config=custom_config)
    page_iterator = result['servicecatalog-appregistry'][0]
    for page in page_iterator:
        service_catalog_appregistry_process_page(page, service_catalog_appregistry_client, session)

    return new_result

"""
Describe EC2 Other - EBS, Snapshot, EIP, NAT Gateway, Internet Gateway, VPC, Subnet, Route Table.
"""
def ec2_process_volumes(volume, ec2_client, session, log):
    new_resource = {}
    new_resource['AccountId'] = session.client('sts').get_caller_identity()['Account']
    new_resource['Region'] = session.region_name
    new_resource['ResourceId'] = volume['VolumeId']
    new_resource['Tags'] = ec2_client.describe_tags(Filters=[{'Name': 'resource-id', 'Values': [new_resource['ResourceId']]}])['Tags']
    return new_resource

def ec2_process_snapshots(snapshot, ec2_client, session, log):
    new_resource = {}
    new_resource['AccountId'] = session.client('sts').get_caller_identity()['Account']
    new_resource['Region'] = session.region_name
    new_resource['ResourceId'] = snapshot['SnapshotId']
    new_resource['Tags'] = ec2_client.describe_tags(Filters=[{'Name': 'resource-id', 'Values': [new_resource['ResourceId']]}])['Tags']
    return new_resource

def ec2_process_nat_gateway(nat_gateway, ec2_client, session, log):
    new_resource = {}
    new_resource['AccountId'] = session.client('sts').get_caller_identity()['Account']
    new_resource['Region'] = session.region_name
    new_resource['ResourceId'] = nat_gateway['NatGatewayId']
    new_resource['Tags'] = ec2_client.describe_tags(Filters=[{'Name': 'resource-id', 'Values': [new_resource['ResourceId']]}])['Tags']
    return new_resource

def ec2_process_internet_gateway(internet_gateway, ec2_client, session, log):
    new_resource = {}
    new_resource['AccountId'] = session.client('sts').get_caller_identity()['Account']
    new_resource['Region'] = session.region_name
    new_resource['ResourceId'] = internet_gateway['InternetGatewayId']
    new_resource['Tags'] = ec2_client.describe_tags(Filters=[{'Name': 'resource-id', 'Values': [new_resource['ResourceId']]}])['Tags']
    return new_resource

def ec2_process_addresses(address, ec2_client, session, log):
    new_resource = {}
    new_resource['AccountId'] = session.client('sts').get_caller_identity()['Account']
    new_resource['Region'] = session.region_name
    new_resource['ResourceId'] = address['PublicIp']
    new_resource['Tags'] = address['Tags']
    return new_resource

def ec2_process_vpcs(vpc, ec2_client, session, log):
    new_resource = {}
    new_resource['AccountId'] = session.client('sts').get_caller_identity()['Account']
    new_resource['Region'] = session.region_name
    new_resource['ResourceId'] = vpc['VpcId']
    new_resource['Tags'] = ec2_client.describe_tags(Filters=[{'Name': 'resource-id', 'Values': [new_resource['ResourceId']]}])['Tags']
    return new_resource

def ec2_process_subnets(subnet, ec2_client, session, log):
    new_resource = {}
    new_resource['AccountId'] = session.client('sts').get_caller_identity()['Account']
    new_resource['Region'] = session.region_name
    new_resource['ResourceId'] = subnet['SubnetId']
    new_resource['Tags'] = ec2_client.describe_tags(Filters=[{'Name': 'resource-id', 'Values': [new_resource['ResourceId']]}])['Tags']
    return new_resource

def ec2_process_route_tables(route_table, ec2_client, session, log):
    new_resource = {}
    new_resource['AccountId'] = session.client('sts').get_caller_identity()['Account']
    new_resource['Region'] = session.region_name
    new_resource['ResourceId'] = route_table['RouteTableId']
    new_resource['Tags'] = ec2_client.describe_tags(Filters=[{'Name': 'resource-id', 'Values': [new_resource['ResourceId']]}])['Tags']
    return new_resource

def get_ec2_other(result, session, region, log):
    new_result = {'ec2_other': {}}
    def ec2_process_page(page, ec2_client, session):
        if 'Volumes' in page:
            if 'volumes' not in new_result['ec2_other']:
                new_result['ec2_other']['volumes'] = []
            volumes = page['Volumes']
            with concurrent.futures.ThreadPoolExecutor() as executor:
                futures = []
                for volume in volumes:
                    future = executor.submit(ec2_process_volumes, volume, ec2_client, session, log)
                    futures.append(future)
                for future in concurrent.futures.as_completed(futures):
                    new_resource = future.result()
                    new_result['ec2_other']['volumes'].append(new_resource)
        elif 'Snapshots' in page:
            snapshots = page['Snapshots']
            if 'snapshots' not in new_result['ec2_other']:
                new_result['ec2_other']['snapshots'] = []
            with concurrent.futures.ThreadPoolExecutor() as executor:
                futures = []
                for snapshot in snapshots:
                    future = executor.submit(ec2_process_snapshots, snapshot, ec2_client, session, log)
                    futures.append(future)
                for future in concurrent.futures.as_completed(futures):
                    new_resource = future.result()
                    new_result['ec2_other']['snapshots'].append(new_resource)
        elif 'NatGateways' in page:
            nat_gateways = page['NatGateways']
            if 'nat_gateways' not in new_result['ec2_other']:
                new_result['ec2_other']['nat_gateways'] = []
            with concurrent.futures.ThreadPoolExecutor() as executor:
                futures = []
                for nat_gateway in nat_gateways:
                    future = executor.submit(ec2_process_nat_gateway, nat_gateway, ec2_client, session, log)
                    futures.append(future)
                for future in concurrent.futures.as_completed(futures):
                    new_resource = future.result()
                    new_result['ec2_other']['nat_gateways'].append(new_resource)
        elif 'InternetGateways' in page:
            internet_gateways = page['InternetGateways']
            if 'internet_gateways' not in new_result['ec2_other']:
                new_result['ec2_other']['internet_gateways'] = []
            with concurrent.futures.ThreadPoolExecutor() as executor:
                futures = []
                for internet_gateway in internet_gateways:
                    future = executor.submit(ec2_process_internet_gateway, internet_gateway, ec2_client, session, log)
                    futures.append(future)
                for future in concurrent.futures.as_completed(futures):
                    new_resource = future.result()
                    new_result['ec2_other']['internet_gateways'].append(new_resource)
        elif 'Addresses' in page:
            addresses = page['Addresses']
            if 'addresses' not in new_result['ec2_other']:
                new_result['ec2_other']['addresses'] = []
            with concurrent.futures.ThreadPoolExecutor() as executor:
                futures = []
                for address in addresses:
                    future = executor.submit(ec2_process_addresses, address, ec2_client, session, log)
                    futures.append(future)
                for future in concurrent.futures.as_completed(futures):
                    new_resource = future.result()
                    new_result['ec2_other']['addresses'].append(new_resource)
        elif 'Vpcs' in page:
            vpcs = page['Vpcs']
            if 'vpcs' not in new_result['ec2_other']:
                new_result['ec2_other']['vpcs'] = []
            with concurrent.futures.ThreadPoolExecutor() as executor:
                futures = []
                for vpc in vpcs:
                    future = executor.submit(ec2_process_vpcs, vpc, ec2_client, session, log)
                    futures.append(future)
                for future in concurrent.futures.as_completed(futures):
                    new_resource = future.result()
                    new_result['ec2_other']['vpcs'].append(new_resource)
        elif 'Subnets' in page:
            subnets = page['Subnets']
            if 'subnets' not in new_result['ec2_other']:
                new_result['ec2_other']['subnets'] = []
            with concurrent.futures.ThreadPoolExecutor() as executor:
                futures = []
                for subnet in subnets:
                    future = executor.submit(ec2_process_subnets, subnet, ec2_client, session, log)
                    futures.append(future)
                for future in concurrent.futures.as_completed(futures):
                    new_resource = future.result()
                    new_result['ec2_other']['subnets'].append(new_resource)
        elif 'RouteTables' in page:
            route_tables = page['RouteTables']
            if 'route_tables' not in new_result['ec2_other']:
                new_result['ec2_other']['route_tables'] = []
            with concurrent.futures.ThreadPoolExecutor() as executor:
                futures = []
                for route_table in route_tables:
                    future = executor.submit(ec2_process_route_tables, route_table, ec2_client, session, log)
                    futures.append(future)
                for future in concurrent.futures.as_completed(futures):
                    new_resource = future.result()
                    new_result['ec2_other']['route_tables'].append(new_resource)
    max_connections = 100
    custom_config = Config(max_pool_connections=max_connections, retries = {'max_attempts': 10, 'mode': 'standard'})
    ec2_client = session.client('ec2', region_name=region, config=custom_config)
    page_iterator_objects = result['ec2']
    for page_iterator_object in page_iterator_objects:
        #log.info(f"EC2 Page Iterator Object Loop: {page_iterator_object}")
        for page in page_iterator_object:
            #log.info(f"EC2 Page Loop: {page}")
            ec2_process_page(page, ec2_client, session)

    return new_result


"""
Describe all AWS resources:
DynamoDB
S3
Lambda
AppSync
DirectConnect
ELB
ELBv2
Events
SQS
Kinesis
kinesis firehose
Step Functions - State Machines
Connect
CloudWatch
Cloudwatch Logs
X-ray
Config
EC2 Other - Volume, Snapshot, EIP, NAT Gateway, Internet Gateway
KMS
Secrets Manager
Service Catalog
Service Catalog App Registry
"""

def describe_resources(result, session, region, log):
    log.info("Started: AWS Describe Resources")
    new_result = {}
    try:
        for resource in result:
            if resource == 'dynamodb':
                tables = get_dynamodb_tables(result, session, region, log)
                new_result['dynamodb'] = tables['dynamodb']
            elif resource == 's3':
                buckets = get_s3_buckets(result, session, region, log)
                new_result['s3'] = buckets['s3']
            elif resource == 'lambda':
                functions = get_lambda_functions(result, session, region, log)
                new_result['lambda'] = functions['lambda']
            elif resource == 'appsync':
                functions = get_appsync_graphql_api(result, session, region, log)
                new_result['appsync'] = functions['appsync']
            elif resource == 'directconnect':
                connections = get_directconnect_connections(result, session, region, log)
                new_result['directconnect'] = connections['directconnect']
            elif resource == 'elb':
                load_balancers = get_elb_load_balancers(result, session, region, log)
                new_result['elb'] = load_balancers['elb']
            elif resource == 'elbv2':
                load_balancers = get_elbv2_load_balancers(result, session, region, log)
                new_result['elbv2'] = load_balancers['elbv2']
            elif resource == 'events':
                events = get_events(result, session, region, log)
                new_result['events'] = events['events']
            elif resource == 'sqs':
                queues = get_sqs_queues(result, session, region, log)
                new_result['sqs'] = queues['sqs']
            elif resource == 'kinesis':
                streams = get_kinesis_streams(result, session, region, log)
                new_result['kinesis'] = streams['kinesis']
            elif resource == 'firehose':
                print(f"Processing Service: {resource}")
                streams = get_firehose_streams(result, session, region, log)
                new_result['firehose'] = streams['firehose']
            elif resource == 'stepfunctions':
                machines = get_state_machines(result, session, region, log)
                new_result['stepfunctions'] = machines['stepfunctions']
            elif resource == 'connect':
                connections = get_connect_instances(result, session, region, log)
                new_result['connect'] = connections['connect']
            elif resource == 'cloudwatch':
                alarms = get_cloudwatch_alarms(result, session, region, log)
                new_result['cloudwatch'] = alarms['cloudwatch']
            elif resource == 'logs':
                alarms = get_log_groups(result, session, region, log)
                new_result['logs'] = alarms['logs']
            elif resource == 'xray':
                groups = get_xray_groups(result, session, region, log)
                new_result['xray'] = groups['xray']
            elif resource == 'config':
                rules = get_config_rules(result, session, region, log)
                new_result['config'] = rules['config']
            elif resource == 'ec2':
                ec2_other = get_ec2_other(result, session, region, log)
                new_result['ec2'] = ec2_other['ec2_other']
            elif resource == 'kms':
                keys = get_kms_keys(result, session, region, log)
                new_result['kms'] = keys['kms']
            elif resource == 'secretsmanager':
                secrets = get_secrets_manager_secrets(result, session, region, log)
                new_result['secretsmanager'] = secrets['secretsmanager']
            elif resource == 'servicecatalog':
                products = get_servicecatalog_products(result, session, region, log)
                new_result['servicecatalog'] = products['servicecatalog']
            elif resource == 'servicecatalog-appregistry':
                apps = get_servicecatalog_appregistry_apps(result, session, region, log)
                new_result['servicecatalog-appregistry'] = apps['servicecatalog-appregistry']
            else:
                print(f"Service {resource} not supported")
                return
    except Exception as exc:
        log.error(f"Error describing resources: {exc}")
        log.error(traceback.format_exc())
    
    log.info("Finished: AWS Describe Resources")
    return new_result
    
def main(
    scan,
    regions,
    output_dir,
    log_level,
    max_retries,
    retry_delay,
    concurrent_regions,
    concurrent_services,
):
    """
    Main function to perform the AWS services scan.

    Arguments:
    scan -- The path to the JSON file or URL containing the AWS services to scan.
    regions -- The AWS regions to scan.
    output_dir -- The directory to store the results.
    log_level -- The log level for the script.
    max_retries -- The maximum number of retries for each service.
    retry_delay -- The delay before each retry.
    concurrent_regions -- The number of regions to process concurrently.
    concurrent_services -- The number of services to process concurrently for each region.
    """

    session = boto3.Session()
    if not check_aws_credentials(session):
        print("Invalid AWS credentials. Please configure your credentials.")
        return

    log = setup_logging(output_dir, log_level)

    if scan.startswith("http://") or scan.startswith("https://"):
        services = get_json_from_url(scan)
        if services is None:
            print(f"Failed to load services from {scan}. Exiting.")
            return
    else:
        with open(scan, "r") as f:
            services = json.load(f)
    if not regions:
        ec2_client = session.client("ec2")
        regions = [
            region["RegionName"]
            for region in ec2_client.describe_regions()["Regions"]
            if region["OptInStatus"] == "opt-in-not-required"
            or region["OptInStatus"] == "opted-in"
        ]

    start_time = time.time()

    with concurrent.futures.ThreadPoolExecutor(
        max_workers=concurrent_regions
    ) as executor:
        future_to_region = {
            executor.submit(
                process_region,
                region,
                services,
                session,
                log,
                max_retries,
                retry_delay,
                concurrent_services,
            ): region
            for region in regions
        }
        for future in concurrent.futures.as_completed(future_to_region):
            region = future_to_region[future]
            results = {}
            new_results = {}
            try:
                region_results = future.result()
                for service_result in region_results:
                    if service_result['service'] not in results:
                        results[service_result['service']] = []
                        results[service_result['service']].append(service_result["result"])
                    else:
                        results[service_result['service']].append(service_result["result"])
                new_results = describe_resources(results, session, region, log)
                directory = os.path.join(output_dir, timestamp, region)
                os.makedirs(directory, exist_ok=True)
                with open(
                    os.path.join(directory, f"{service_result['service']}.json"),
                    "w",
                ) as f:
                    json.dump(new_results, f, indent=2, cls=DateTimeEncoder)
            except Exception as exc:
                log.error("%r generated an exception: %s" % (region, exc))
                log.error(traceback.format_exc())
    end_time = time.time()
    elapsed_time = end_time - start_time
    log.info(f"Total elapsed time for scanning: {display_time(elapsed_time)}")
    print(f"Total elapsed time for scanning: {display_time(elapsed_time)}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="List all resources in all AWS services and regions."
    )
    parser.add_argument(
        "-s",
        "--scan",
        help="The path to the JSON file or URL containing the AWS services to scan.",
        required=True,
    )
    parser.add_argument(
        "-r", "--regions", nargs="+", help="List of AWS regions to scan"
    )
    parser.add_argument(
        "-o", "--output_dir", default="output", help="Directory to store the results"
    )
    parser.add_argument(
        "-l",
        "--log_level",
        default="INFO",
        help="Set the logging level (e.g., DEBUG, INFO, WARNING, ERROR, CRITICAL)",
    )
    # New arguments
    parser.add_argument(
        "--max-retries",
        type=int,
        default=3,
        help="Maximum number of retries for each service",
    )
    parser.add_argument(
        "--retry-delay",
        type=int,
        default=2,
        help="Delay (in seconds) before each retry",
    )
    parser.add_argument(
        "--concurrent-regions",
        type=int,
        default=None,
        help="Number of regions to process concurrently. Default is None, which means the script will use as many as possible",
    )
    parser.add_argument(
        "--concurrent-services",
        type=int,
        default=None,
        help="Number of services to process concurrently for each region. Default is None, which means the script will use as many as possible",
    )
    args = parser.parse_args()
    main(
        args.scan,
        args.regions,
        args.output_dir,
        args.log_level,
        args.max_retries,
        args.retry_delay,
        args.concurrent_regions,
        args.concurrent_services,
    )