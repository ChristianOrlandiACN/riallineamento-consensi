import json
import boto3

_client = boto3.client("secretsmanager", region_name="eu-west-1")
_cache: dict = {}


def get_db_credentials(secret_name: str) -> dict:
    if secret_name in _cache:
        return _cache[secret_name]
    response = _client.get_secret_value(SecretId=secret_name)
    secret = json.loads(response["SecretString"])
    _cache[secret_name] = secret
    return secret
