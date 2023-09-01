import concurrent.futures
from botocore.exceptions import ThrottlingException
from botocore.config import Config
import time

def lambda_apply_backoff_with_retry(api_call, max_retries=5, base_backoff_time=1):
    for retry in range(max_retries + 1):
        try:
            return api_call()
        except ThrottlingException:
            if retry < max_retries:
                print(f"ThrottlingException encountered, retrying ({retry + 1}/{max_retries + 1})")
                backoff_time = base_backoff_time * (2 ** retry)
                print(f"Waiting for {backoff_time} seconds before retrying...")
                time.sleep(backoff_time)
            else:
                print("Max retries reached for ThrottlingException")
                raise

def lambda_get_tags_with_backoff(api_call):
    return lambda_apply_backoff_with_retry(api_call)

def lambda_get_function_configuration_with_retry(lambda_client, function_name, max_retries=5, base_backoff_time=1):
    for retry in range(max_retries + 1):
        try:
            return lambda_client.get_function_configuration(FunctionName=function_name)
        except ThrottlingException:
            if retry < max_retries:
                print(f"ThrottlingException encountered for get_function_configuration, retrying ({retry + 1}/{max_retries + 1})")
                backoff_time = base_backoff_time * (2 ** retry)
                print(f"Waiting for {backoff_time} seconds before retrying...")
                time.sleep(backoff_time)
            else:
                print("Max retries reached for ThrottlingException")
                raise

def lambda_process_function(function, lambda_client, session):
    response = lambda_get_function_configuration_with_retry(lambda_client, function['FunctionName'])
    new_function = {}
    new_function['AccountID'] = session.client('sts').get_caller_identity().get('Account')
    new_function['Region'] = session.region_name
    new_function['Arn'] = response['FunctionArn']
    
    # Handle ThrottlingException with exponential backoff retry for list_tags
    def lambda_get_tags():
        return lambda_client.list_tags(Resource=new_function['Arn'])['Tags']
    
    new_function['Tags'] = lambda_get_tags_with_backoff(lambda_get_tags)

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
    custom_config = Config(max_pool_connections=max_connections)
    lambda_client = session.client('lambda', region_name=region, config=custom_config)
    page_iterator = result['lambda'][0]
    for page in page_iterator:
        lambda_process_page(page, lambda_client, session)
    
    return new_result
