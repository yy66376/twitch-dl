import json
from datetime import datetime

import m3u8
import requests
from requests import RequestException

from src.constants import TWITCH_VIDEO_URL, TWITCH_GQL_URL, USHER_API_URL, CLIENT_ID


def get_vod_info(vod_id: str) -> dict:
    """
    Retrieves information about the specified VOD including length in seconds, title, channel name, and the date
    the VOD was published as a dictionary.
    """
    response = requests.get(TWITCH_VIDEO_URL.format(vod_id=vod_id),
                            headers={'Accept': 'application/vnd.twitchtv.v5+json', 'Client-ID': CLIENT_ID})
    if response.status_code == 200:
        json_response = response.json()
        return {'length': json_response['length'], 'title': json_response['title'],
                'channel': json_response['channel']['name'],
                'date': datetime.strptime(json_response['created_at'], '%Y-%m-%dT%H:%M:%SZ')}
    elif response.status_code == 404:
        raise ValueError(f"The specified VOD ID {vod_id} does not exist on Twitch or has been deleted.")
    else:
        raise RequestException(f"Twitch server returned with status code {response.status_code}.")


def get_vod_gql_query(vod_id: str):
    """
    Constructs a json representation of a Twitch GQL VOD access query.
    """
    gql_access_token_query_dict = {
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
    return json.dumps(gql_access_token_query_dict, indent=None)


def get_vod_access_token(vod_id: str) -> dict:
    """
    Retrieves the access token value and signature for the VOD stored in a dictionary.
    """
    query = get_vod_gql_query(vod_id)
    response = requests.post(TWITCH_GQL_URL, data=query, headers={"Client-ID": CLIENT_ID})
    if response.status_code == 200:
        video_playback_access_token = response.json()['data']['videoPlaybackAccessToken']
        return {"value": video_playback_access_token['value'],
                "signature": video_playback_access_token['signature']}
    else:
        raise RequestException(f"Twitch GQL server returned with status code {response.status_code}")


def get_vod_playlist_urls(self):
    """
    Retrieves the m3u8 playlist URLs for this VOD stored in a dictionary.
    Keys are video resolutions and values are tuples of video resolution and the playlist URL corresponding
    to that video resolution.
    """
    url = USHER_API_URL.format(vod_id=self.vod_id)
    access_token = get_vod_access_token(vod_id=self.vod_id)
    response = requests.get(url,
                            params={"client_id": CLIENT_ID, "allow_source": True, "token": access_token['value'],
                                    "sig": access_token['signature']})
    if response.status_code == 200:
        variant_playlists = m3u8.loads(response.text)
        quality_to_resolution_url = {}
        for playlist in variant_playlists.playlists:
            resolution = playlist.stream_info.resolution
            quality_to_resolution_url[playlist.stream_info.video] = (
                str(resolution[0]) + '×' + str(resolution[1]), playlist.uri)
        return quality_to_resolution_url
    else:
        raise RequestException(f"Twitch usher server returned with status code {response.status_code}")
