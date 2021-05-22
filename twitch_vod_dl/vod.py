import os
import requests
import json
import m3u8
import datetime
import urllib.request
import shutil
import threading
import subprocess
from requests.exceptions import RequestException

CLIENT_ID = "kimne78kx3ncx6brgo4mv6wki5h1ko"

# These API endpoints are not officially supported by Twitch and may break at any time.
TWITCH_GQL_URL = "https://gql.twitch.tv/gql"
USHER_API_URL = "https://usher.ttvnw.net/vod/{vod_id}"

# Official Twitch API endpoints
TWITCH_VIDEO_URL = "https://api.twitch.tv/kraken/videos/{vod_id}"

# Regex for matching .ts files
TS_REGEX = "\d+\.ts$"


def validate_time(time: int, low: int, high: int):
    """
    Ensures that time is within bounds of (low, high). Throws an exception otherwise.
    """
    if time < low or time > high:
        raise ValueError("Time is out of range. Please enter a correct time.")


def time_to_seconds(time: str) -> int:
    """
    Converts a string representation of time [hh mm ss] to an integer in seconds
    """
    components = time.split()
    if len(components) != 3:
        raise ValueError("Please enter time in the format [hh mm ss].")
    else:
        converted_time = 0
        components = [int(component) for component in components]
        validate_time(components[0], 0, 999)
        validate_time(components[1], 0, 59)
        validate_time(components[2], 0, 59)
        converted_time += 3600 * components[0]
        converted_time += 60 * components[1]
        converted_time += components[2]
        return converted_time


class VOD(object):

    def __init__(self, vod_id, start_time="", end_time="", quality=""):
        self.vod_id = vod_id
        self.length = -1
        self.title = ""
        self.channel = ""
        self.date = None
        self.get_vod_info()

        self.playlist_urls = self.get_playlist_urls()
        self.quality_options = self.get_quality_options()
        if quality != "" and quality not in self.quality_options:
            raise ValueError("Please enter a valid quality option.")
        else:
            self.quality = self.quality_options[0]
        self.playlist_url = self.playlist_urls[self.quality][1]
        self.playlist_prefix = self.playlist_url.split('index-dvr.m3u8')[0]

        self.first_chunk = -1
        self.last_chunk = -1

        self.first_chunk_start = -1
        self.first_chunk_end = -1
        self.last_chunk_start = -1
        self.last_chunk_end = -1

        self.start_time = 0 if start_time == "" else time_to_seconds(start_time)
        validate_time(self.start_time, 0, self.length)
        self.end_time = self.length if end_time == "" else time_to_seconds(end_time)
        validate_time(self.end_time, self.start_time, self.length)

        filename = self.download_playlist()
        self.resolve_first_last_chunks(filename)

    def get_vod_info(self):
        response = requests.get(TWITCH_VIDEO_URL.format(vod_id=self.vod_id),
                                headers={'Accept': 'application/vnd.twitchtv.v5+json', 'Client-ID': CLIENT_ID})
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

    def download_playlist(self) -> str:
        """Downloads the m3u8 playlist file to the cwd."""
        filename, _ = urllib.request.urlretrieve(self.playlist_url, f'{self.channel}_{self.vod_id}.m3u8')
        return filename

    def resolve_first_last_chunks(self, filename: str):
        """Determines the first and last chunks of the video."""
        m3u8_fh = m3u8.load(filename)

        curr_time = 0
        for index, segment in enumerate(m3u8_fh.data['segments']):
            old_time = curr_time
            curr_time += segment['duration']

            if self.first_chunk == -1 and self.start_time < curr_time:
                self.first_chunk = index
                self.first_chunk_start = self.start_time - old_time
                self.first_chunk_end = segment['duration']

            if self.last_chunk == -1 and self.end_time <= curr_time:
                self.last_chunk = index
                self.last_chunk_start = 0
                self.last_chunk_end = self.end_time - old_time
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

    def get_vod_gql_query(self):
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
                "vodID": self.vod_id,
                "playerType": "embed"
            },
        }
        return json.dumps(gql_access_token_query_dict, indent=None)

    def get_vod_access_token(self) -> dict:
        """
        Retrieves the access token value and signature for this VOD stored in a dictionary.
        """
        query = self.get_vod_gql_query()
        response = requests.post(TWITCH_GQL_URL, data=query, headers={"Client-ID": CLIENT_ID})
        if response.status_code == 200:
            video_playback_access_token = response.json()['data']['videoPlaybackAccessToken']
            return {"value": video_playback_access_token['value'],
                    "signature": video_playback_access_token['signature']}
        else:
            raise RequestException(f"Twitch GQL server returned with status code {response.status_code}")

    def get_playlist_urls(self):
        """
        Retrieves the m3u8 playlist URLs for this VOD stored in a dictionary.
        Keys are video resolutions and values are tuples of video resolution and the playlist URL corresponding
        to that video resolution.
        """
        url = USHER_API_URL.format(vod_id=self.vod_id)
        access_token = self.get_vod_access_token()
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


# def download_concurrent(vod: VOD, num_threads: int = 8):
#     pending_jobs = queue.Queue()

#     # Add all download jobs to the queue
#     for chunk in range(vod.first_chunk, vod.last_chunk + 1):
#         pending_jobs.put(chunk)

#     # Create the file that all the threads will be writing to
#     fh = open(f'{vod.channel}_{vod.vod_id}.ts', 'wb')

#     # Create a pool of threads
#     thread_list = []
#     for i in range(num_threads):
#         thread = threading.Thread(target=worker_handler, args=(vod, pending_jobs, fh))
#         thread_list.append(thread)
#         thread.start()

#     # Block until all download jobs are complete
#     pending_jobs.join()

# def worker_handler(vod: VOD, queue: queue.Queue, fh):
#     while True:
#         chunk_num = queue.get()

#         response = requests.get(vod.playlist_prefix + str(chunk_num) + '.ts')

#         if response.status_code == 200:
#             fh.write(response.content)


#         queue.join()

def download_concurrent(vod: VOD, num_threads: int = 8):
    # Download and trim the first and last chunk of the video

    vod_file_name = f'{vod.channel}_{vod.vod_id}.ts'
    first_chunk_file_name = f"{vod.vod_id}_global_first_chunk.ts"
    last_chunk_file_name = f"{vod.vod_id}_global_last_chunk.ts"

    if vod.first_chunk != vod.last_chunk:
        download_chunk_and_trim(vod, first_chunk_file_name, vod.first_chunk, vod.first_chunk_start, vod.first_chunk_end)
        download_chunk_and_trim(vod, last_chunk_file_name, vod.last_chunk, vod.last_chunk_start, vod.last_chunk_end)
    else:
        download_chunk_and_trim(vod, vod_file_name, vod.first_chunk, vod.first_chunk_start, vod.last_chunk_end)
        return
    vod.first_chunk += 1
    vod.last_chunk -= 1

    # Determine the number of chunks each thread will download
    num_chunks = (vod.last_chunk - vod.first_chunk + 1) // num_threads
    remainder = (vod.last_chunk - vod.first_chunk + 1) % num_threads
    print("Number of chunks: ", num_chunks)
    print("Remainder: ", remainder)
    counter = vod.first_chunk

    # Create a pool of threads
    thread_list = []
    worker_done = []
    if vod.last_chunk - vod.first_chunk + 1 < num_threads:
        num_threads = vod.last_chunk - vod.first_chunk + 1
    for worker_id in range(num_threads):
        thread = threading.Thread(target=worker_handler, args=(
            worker_id, num_threads, vod, counter, counter + num_chunks + (1 if remainder > 0 else 0), worker_done))
        counter += num_chunks + (1 if remainder > 0 else 0)
        remainder -= 1
        thread_list.append(thread)
        worker_done.append(threading.Semaphore(value=0))
        thread.start()

    with open(first_chunk_file_name, 'ab') as final_file:

        # Append all intermediate chunks from workers to the global first chunk
        for next_worker in range(num_threads):
            worker_done[next_worker].acquire()
            with open(f'{vod.channel}_{vod.vod_id}_{next_worker}.ts', 'rb') as temp_file:
                shutil.copyfileobj(temp_file, final_file)
            os.remove(f'{vod.channel}_{vod.vod_id}_{next_worker}.ts')

        # Finally, append the global last chunk
        with open(last_chunk_file_name, 'rb') as temp_file:
            shutil.copyfileobj(temp_file, final_file)
        os.remove(last_chunk_file_name)
    shutil.move(first_chunk_file_name, vod_file_name)


def download_chunk_and_trim(vod: VOD, file_name: str, target_chunk: int, start: int, end: int):
    """Downloads a chunk of the vod and trims it from start (in seconds) to end (in seconds)"""
    response = requests.get(vod.playlist_prefix + str(target_chunk) + '.ts')
    if response.status_code == 200:
        with open(f'{vod.vod_id}.ts', 'wb') as fh:
            fh.write(response.content)
        subprocess.run(['ffmpeg', '-y', '-ss', f'{start}', '-i', f'{vod.vod_id}.ts', '-to', f'{end}', '-c', 'copy',
                        f'{file_name}'])
        os.remove(f'{vod.vod_id}.ts')
    else:
        raise RequestException(
            f'Failed to download first chunk of video: server returned with status code {response.status_code}')


def worker_handler(worker_id: int, num_workers: int, vod: VOD, first_chunk: int, last_chunk: int, workers_done: list):
    """Downloads chunk #first_chunk (inclusive) to chunk #last_chunk (exclusive) of the vod."""
    with open(f'{vod.channel}_{vod.vod_id}_{worker_id}.ts', 'ab') as fh:
        for chunk_num in range(first_chunk, last_chunk):
            response = requests.get(vod.playlist_prefix + str(chunk_num) + '.ts')
            if response.status_code == 200:
                fh.write(response.content)
            else:
                raise RequestException(
                    f"Failed to download chunk #{chunk_num}: server returned with status code {response.status_code}")
    workers_done[worker_id].release()


if __name__ == '__main__':
    v = VOD("1015165464", "6 0 10", "6 0 40")
    download_concurrent(vod=v, num_threads=8)
