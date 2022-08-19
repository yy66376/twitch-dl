import json
import re
from datetime import datetime
import pytest
from unittest.mock import patch
import vcr
from requests import RequestException

from src.services.twitch import get_vod_gql_query, get_vod_access_token, contact_usher_api, \
    get_vod_playlist_urls, TwitchAPI

my_vcr = vcr.VCR(
    cassette_library_dir='fixtures/cassettes',
    record_mode='once',
    path_transformer=vcr.VCR.ensure_suffix('.yaml')
)


class TestOfficialTwitch:
    @my_vcr.use_cassette()
    @pytest.mark.usefixtures('permanent_live_vod_id', 'fake_twitch_api_response')
    def test_get_vod_info_status_200(self, permanent_live_vod_id, fake_twitch_api_response):
        assert TwitchAPI.get_vod_info(permanent_live_vod_id) == fake_twitch_api_response

    @pytest.mark.usefixtures('vod_id', 'mock_get')
    def test_get_vod_info_status_404(self, vod_id, mock_get):
        with pytest.raises(ValueError, match=re.escape(f"The specified VOD ID {vod_id} does not exist on Twitch or "
                                                       f"has been deleted.")):
            mock_get.return_value.status_code = 404
            TwitchAPI.get_vod_info(vod_id)

    @pytest.mark.usefixtures('vod_id', 'mock_get')
    def test_get_vod_info_non_404_or_200_status(self, vod_id, mock_get):
        with pytest.raises(RequestException, match="^Twitch server returned with status code .*"):
            mock_get.return_value.status_code = 400
            TwitchAPI.get_vod_info(vod_id)


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

    @my_vcr.use_cassette()
    @pytest.mark.usefixtures('permanent_live_vod_id')
    def test_get_vod_access_token_status_200(self, permanent_live_vod_id, ):
        gql_response = get_vod_access_token(permanent_live_vod_id)
        # signature changes in each gql request and value may change in each request, so just check that they exist
        assert 'value' in gql_response and 'signature' in gql_response

    @pytest.mark.usefixtures('vod_id', 'mock_post')
    def test_get_vod_access_token_non_status_200(self, vod_id, mock_post):
        with pytest.raises(RequestException, match="^Twitch GQL server returned with status code .*"):
            mock_post.return_value.status_code = 404
            get_vod_access_token(vod_id)

    @my_vcr.use_cassette()
    @pytest.mark.usefixtures('permanent_live_vod_id', 'fake_usher_api_response')
    def test_contact_usher_api_status_200(self, permanent_live_vod_id, fake_usher_api_response):
        # remove the second line
        usher_api_response = contact_usher_api(permanent_live_vod_id).splitlines(keepends=True)
        usher_api_response = usher_api_response[:1] + usher_api_response[2:]
        usher_api_response = ''.join(usher_api_response)

        assert usher_api_response == fake_usher_api_response

    @pytest.mark.usefixtures('vod_id', 'mock_get')
    def test_contact_usher_api_non_status_200(self, vod_id, mock_get, mock_get_vod_access_token):
        with pytest.raises(RequestException, match="^Twitch usher server returned with status code .*"):
            mock_get_vod_access_token.return_value = {'value': "", "signature": ""}
            mock_get.return_value.status_code = 404
            contact_usher_api(vod_id)

    @pytest.mark.usefixtures('permanent_live_vod_id', 'fake_usher_api_response', 'mock_contact_usher_api')
    def test_get_vod_playlist_urls(self, permanent_live_vod_id, fake_usher_api_response, mock_contact_usher_api,
                                   fake_playlist_urls):
        mock_contact_usher_api.return_value = fake_usher_api_response
        assert get_vod_playlist_urls(permanent_live_vod_id) == fake_playlist_urls


@pytest.fixture()
def vod_id():
    return '000000'


@pytest.fixture()
def mock_get():
    with patch('src.services.twitch.requests.get') as mock_get:
        yield mock_get


@pytest.fixture()
def mock_post():
    with patch('src.services.twitch.requests.post') as mock_post:
        yield mock_post


@pytest.fixture()
def mock_get_vod_access_token():
    with patch('src.services.twitch.get_vod_access_token') as mock_get_vod_access_token:
        yield mock_get_vod_access_token


@pytest.fixture()
def mock_contact_usher_api():
    with patch('src.services.twitch.contact_usher_api') as mock_contact_usher_api:
        yield mock_contact_usher_api


@pytest.fixture()
def permanent_live_vod_id():
    return '502435183'  # id of a highlight vod that's on the sneakylol's twitch channel


@pytest.fixture()
def fake_twitch_api_response():
    """Returns a pre-recorded Twitch API response, keeping only the relevant information."""
    with open('resources/test_official_twitch_api_response.json') as fh:
        json_response = json.load(fh)
        return {'length': json_response['length'], 'title': json_response['title'],
                'channel': json_response['channel']['name'],
                'date': datetime.strptime(json_response['created_at'], '%Y-%m-%dT%H:%M:%SZ')}


@pytest.fixture()
def fake_usher_api_response():
    """Returns a pre-recorded Usher API response with the second line (containing sensitive info) removed."""
    with open('resources/test_usher_api_response.txt') as fh:
        return fh.read()


@pytest.fixture()
def fake_playlist_urls():
    """
    Returns a dictionary of quality codes to tuples of of video resolution and the playlist URL corresponding
    to that video resolution.
    """
    with open('resources/test_playlist_urls.json') as fh:
        d = json.load(fh)
        for key in d:
            d[key] = tuple(d[key])
        return d
