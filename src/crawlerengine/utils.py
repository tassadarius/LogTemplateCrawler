import requests

_api_endpoint = 'https://api.github.com/graphql'


def send_and_receive(header: dict, post_data: dict):
    response = requests.post(_api_endpoint, data=post_data, headers=header)
    if response.status_code != 200:
        print(f'Error {response.status_code}: {response.reason}')
        print(response.text)
        return None
    if 'errors' in response.json():
        raise ValueError(f"GitHub API errors: {response.json()['errors']}")
    else:
        return response


def get_deepest_dict_value(data: dict):
    for k, v in data.items():
        if type(v) == dict:
            return get_deepest_dict_value(v)
        else:
            return v
