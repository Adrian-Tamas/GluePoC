import logging

import boto3

logger = logging.getLogger('cloudrunner-automation')


def get_api_id(api_name: str, region) -> str:
    """
    Get the id of the app deployed in API Gateway under the api_name endpoint
    :param region: aws_client need region to identify the Api Gateway location
    :param api_name: the name of the endpoint in Api Gateway
    :return: the id of the app to be used to get the url
    """
    api_client = boto3.client('apigateway', region_name=region)
    apis = api_client.get_rest_apis(limit=500)
    rest_api_id = [item['id'] for item in apis['items'] if api_name in item["name"]][0]
    return rest_api_id


def get_api_url(api_name: str, region: str, stage: str) -> str:
    """
    Get the base url of the endpoint/app deployed in API Gateway
    :param api_name: the name of the endpoint
    :param region: the aws region it is deployed in
    :param stage: the stage of the deploy (dev, test, etc)
    :return: the url
    """
    api_url = f'https://{get_api_id(api_name, region)}.execute-api.{region}.amazonaws.com/{stage}'
    return api_url
