import requests
import json
import m3u8
import datetime
import urllib.request
import re
from requests.exceptions import RequestException

CLIENT_ID = "kimne78kx3ncx6brgo4mv6wki5h1ko"

# API endpoints are not officially supported by Twitch and may break at any time
TWITCH_GQL_URL = "https://gql.twitch.tv/gql"
USHER_API_URL = "https://usher.ttvnw.net/vod/{vod_id}"

# Official Twitch API endpoints
TWITCH_VIDEO_URL = "https://api.twitch.tv/kraken/videos/{vod_id}"

# Regex for matching .ts files
TS_REGEX = "\d+\.ts$"

# The current working directory
# cwd = 

class VOD(object):

    def __init__(self, vod_id, start_time="", end_time="", quality=""):
        self.vod_id = vod_id
        self.length = -1
        self.title = ""
        self.channel = ""
        self.date = None
        self.get_vod_info()

        self.playlist_urls = get_playlist_urls(self.vod_id)
        self.quality_options = self.get_quality_options()
        if quality != "" and quality not in self.quality_options:
            raise ValueError("Please enter a valid quality option.")
        else:
            self.quality = self.quality_options[0]
        self.playlist_url = self.playlist_urls[self.quality][1]
        self.playlist_prefix = self.playlist_url.split('index-dvr.m3u8')[0]

        self.first_chunk = -1
        self.last_chunk = -1

        self.start_time = 0 if start_time == "" else self.time_to_seconds(start_time)
        self.validate_time(self.start_time, 0, self.length)
        self.end_time = self.length if end_time == "" else self.time_to_seconds(end_time)
        self.validate_time(self.end_time, self.start_time, self.length)

        filename = self.download_playlist()
        self.resolve_first_last_chunks(filename)
    
    def get_vod_info(self):
        response = requests.get(TWITCH_VIDEO_URL.format(vod_id=self.vod_id), headers={'Accept': 'application/vnd.twitchtv.v5+json', 'Client-ID': CLIENT_ID})
        if response.status_code == 200:
            json_response = response.json()
            self.length = json_response['length']
            self.title = json_response['title']
            self.channel = json_response['channel']['name']
            self.date = datetime.datetime.strptime(json_response['created_at'], '%Y-%m-%dT%H:%M:%SZ')
        elif response.status_code == 404:
            raise ValueError("The specified VOD ID does not exist or has been deleted.")
        else:
            raise RequestException(f"Twitch server returned with status code {response.status_code}.")

    def get_quality_options(self):
        return [quality for quality in self.playlist_urls.keys()]

    def time_to_seconds(self, time: str) -> int:
        """Converts a string representation of time hh mm ss to an integer in seconds"""
        components = time.split()
        if len(components) > 3:
            raise ValueError("Please enter time in the format hh mm ss.")
        else:
            converted_time = 0
            components = [int(component) for component in components]
            self.validate_time(components[0], 0, 999)
            self.validate_time(components[1], 0, 59)
            self.validate_time(components[2], 0, 59)
            converted_time += 3600 * components[0]
            converted_time += 60 * components[1]
            converted_time += components[2]
            return converted_time
    
    def validate_time(self, time: int, low: int, high: int) -> bool:
        """Ensures that time is within bounds."""
        if time < low or time > high:
            raise ValueError("Time is out of range. Please enter a correct time.")

    
    def download_playlist(self) -> str:
        """Downloads the m3u8 playlist file to the cwd."""
        filename, headers = urllib.request.urlretrieve(self.playlist_url, f'{self.channel}_{self.vod_id}.m3u8')
        return filename

    def resolve_first_last_chunks(self, filename: str):
        """Determines the first and last chunks of the video."""
        m3u8_fh = m3u8.load(filename)
        
        curr_time = 0
        for index, segment in enumerate(m3u8_fh.data['segments']):
            curr_time += segment['duration']

            if self.first_chunk == -1 and self.start_time < curr_time:
                self.first_chunk = index

            if self.last_chunk == -1 and self.end_time <= curr_time:
                self.last_chunk = index
                break
        
        if self.first_chunk == -1:
            self.first_chunk = 0
        if self.last_chunk == -1:
            self.last_chunk = len(m3u8_fh.data['segments']) - 1

    def download(self):
        """Downloads the VOD at the specified time interval [self.start_time, self.end_time]."""
        with open(f'{self.channel}_{self.vod_id}.ts', 'wb') as fh:
            for chunk_num in range(self.first_chunk, self.last_chunk + 1):
                response = requests.get(self.playlist_prefix + str(chunk_num) + '.ts')
                if response.status_code == 200:
                    fh.write(response.content)
                else:
                    raise RequestException(f"Twitch usher server returned with status code {response.status_code}")


def get_vod_gql_query(vod_id):
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
    gql_access_token_query_json = json.dumps(gql_access_token_query_dict, indent=None)
    return gql_access_token_query_json

def get_vod_access_token(vod_id):
    """Retrieves the access token value and signature for a VOD."""
    query = get_vod_gql_query(vod_id)
    response = requests.post(TWITCH_GQL_URL, data=query, headers={"Client-ID": CLIENT_ID})
    if response.status_code == 200:
        video_playback_access_token = response.json()['data']['videoPlaybackAccessToken']
        return {"value": video_playback_access_token['value'], "signature": video_playback_access_token['signature']}
    else:
        raise RequestException(f"Twitch GQL server returned with status code {response.status_code}")

def get_playlist_urls(vod_id):
    """Retrieves the playlist URLs for a VOD."""
    url = USHER_API_URL.format(vod_id=vod_id)
    access_token = get_vod_access_token(vod_id)
    response = requests.get(url, params={"client_id": CLIENT_ID, "allow_source": True, "token": access_token['value'], "sig": access_token['signature']})
    if response.status_code == 200:
        variant_playlists = m3u8.loads(response.text)
        quality_to_resolution_url = {}
        for playlist in variant_playlists.playlists:
            resolution = playlist.stream_info.resolution
            quality_to_resolution_url[playlist.stream_info.video] = (str(resolution[0]) + '×' + str(resolution[1]), playlist.uri)
        return quality_to_resolution_url
    else:
        raise RequestException(f"Twitch usher server returned with status code {response.status_code}")

v = VOD("1015165464", start_time="6 0 42", end_time="6 1 0")
v.download()
