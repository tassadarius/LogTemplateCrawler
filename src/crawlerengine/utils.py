import requests


class Communicator:
    _api_endpoint = 'https://api.github.com/graphql'

    def __init__(self, use_session=True):
        self._session = False
        if use_session:
            self._session = requests.Session()

    def send_and_receive(self, header: dict, post_data: dict):
        if self._session:
            response = self._session.post(self._api_endpoint, data=post_data, headers=header)
        else:
            response = requests.post(self._api_endpoint, data=post_data, headers=header)

        if response.status_code != 200:
            print(f'Error {response.status_code}: {response.reason}')
            print(response.text)
            return None
        if 'errors' in response.json():
            raise ValueError(f"GitHub API errors: {response.json()['errors']}")
        else:
            return response

    def close_session(self):
        if self._session:
            self._session.close()


def get_deepest_dict_value(data: dict):
    for k, v in data.items():
        if type(v) == dict:
            return get_deepest_dict_value(v)
        else:
            return v
