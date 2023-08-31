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


def api_call_with_retry(client, function_name, parameters, max_retries, retry_delay):
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
                paginator = client.get_paginator(function_name)
                function_to_call = getattr(paginator, 'paginate')
                if parameters:
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
            client, function, parameters, max_retries, retry_delay
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

def describe_resources(result, session, region, log):
    log.info("Started: AWS Describe Resources")
    new_result = {}
    try:
        for resource in result:
            if resource not in new_result:
                new_result[resource] = []
            if resource == 'dynamodb':
                page_iterator=result[resource][0]
                for page in page_iterator:
                    dynamodb_client = session.client('dynamodb', region_name=region)
                    tables = page['TableNames']
                    for table in tables:
                        response = dynamodb_client.describe_table(TableName=table)
                        new_table = {}
                        new_table['AccountID'] = session.client('sts').get_caller_identity().get('Account')
                        new_table['Region'] = session.region_name
                        new_table['Arn'] = response['Table']['TableArn']
                        new_table['Tags'] = dynamodb_client.list_tags_of_resource(ResourceArn=new_table['Arn'])
                        new_result['dynamodb'].append(new_table)
            elif resource == 's3':
                s3_client = session.client('s3', region_name=region)
                buckets = result[resource][0]['Buckets']
                for bucket in buckets:
                    response = s3_client.get_bucket_location(Bucket=bucket['Name'])
                    if response == 'null':
                        response = "us-east-1"
                    new_bucket = {}
                    new_bucket['AccountID'] = session.client('sts').get_caller_identity().get('Account')
                    new_bucket['Location'] = response['LocationConstraint']
                    try:
                        new_bucket['Tags'] = s3_client.get_bucket_tagging(Bucket=bucket['Name'])
                    except botocore.exceptions.ClientError as error:
                        if 'NoSuchTagSet' in str(error):
                            new_bucket['Tags'] = {}
                            pass
                        else:
                            log.info(f"Error getting tags for bucket {bucket['Name']}: {error}")
                            new_bucket['Tags'] = {}
                            pass
                    new_bucket['Arn'] = 'arn:aws:s3:::' + bucket['Name']
                    new_result['s3'].append(new_bucket)
            elif resource == 'lambda':
                page_iterator=result[resource][0]
                for page in page_iterator:
                    lambda_client = session.client('lambda', region_name=region)
                    functions = page['Functions']
                    for function in functions:
                        response = lambda_client.get_function_configuration(FunctionName=function['FunctionName'])
                        new_function = {}
                        new_function['AccountID'] = session.client('sts').get_caller_identity().get('Account')
                        new_function['Region'] = session.region_name
                        new_function['Arn'] = response['FunctionArn']
                        new_function['Tags'] = lambda_client.list_tags(Resource=new_function['Arn'])
                        new_result['lambda'].append(new_function)
            else:
                print(f"Service {result['service']} not supported")
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