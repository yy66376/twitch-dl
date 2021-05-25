import json
import re
from datetime import datetime
import pytest
from unittest.mock import patch
from unittest import TestCase
from requests import RequestException
from src.services.twitch import get_vod_info, get_vod_gql_query


@pytest.mark.usefixtures('vod_id', 'mock_get')
class TestOfficialTwitch:
    def test_get_vod_info_status_200(self, vod_id, mock_get):
        json_response = {'length': 3600, 'title': 'omega gaming', 'channel': {'name': 'twitch'},
                         'created_at': '2021-01-01T02:01:04Z'}
        mock_get.return_value.status_code = 200
        mock_get.return_value.json.return_value = json_response
        TestCase().assertDictEqual(get_vod_info(vod_id), {'length': 3600, 'title': 'omega gaming', 'channel': 'twitch',
                                                          'date': datetime(2021, 1, 1, 2, 1, 4)})

    def test_get_vod_info_status_404(self, vod_id, mock_get):
        with pytest.raises(ValueError, match=re.escape(f"The specified VOD ID {vod_id} does not exist on Twitch or "
                                                       f"has been deleted.")):
            mock_get.return_value.status_code = 404
            get_vod_info(vod_id)

    def test_get_vod_info_non_404_or_200_status(self, vod_id, mock_get):
        with pytest.raises(RequestException, match="^Twitch server returned with status code .*"):
            mock_get.return_value.status_code = 400
            get_vod_info(vod_id)


class TestUnofficialTwitch:
    @pytest.mark.usefixtures('vod_id')
    def test_get_vod_gql_query(self, vod_id):
        json_query = {
            "operationName": "PlaybackAccessToken",
            "extensions": {
                "persistedQuery": {
                    "version": 1,
                    "sha256Hash": "0828119ded1c13477966434e15800ff57ddacf13ba1911c129dc2200705b0712"
                }
            },
            "variables": {
                "isLive": False,
                "login": "",
                "isVod": True,
                "vodID": vod_id,
                "playerType": "embed"
            },
        }
        assert json.loads(get_vod_gql_query(vod_id)) == json_query


@pytest.fixture
def vod_id():
    return '000000'


@pytest.fixture
def mock_get():
    with patch('src.services.twitch.requests.get') as mock_get:
        yield mock_get
