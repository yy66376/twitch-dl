import os
import requests
import json
import m3u8
import urllib.request
import shutil
import threading
import subprocess
from requests.exceptions import RequestException
from src.constants import TWITCH_VIDEO_URL, TWITCH_GQL_URL, USHER_API_URL, CLIENT_ID
from src.services.twitch import get_vod_info
from src.services.twitch import get_vod_access_token


def validate_time(time: int, low: int, high: int):
    """
    Ensures that time is within bounds of (low, high). Throws a ValueError otherwise.
    """
    if time < low or time > high:
        raise ValueError("Time is out of range. Please enter a correct time.")


def time_to_seconds(time: str) -> int:
    """
    Converts a string representation of time [hh mm ss] to an integer in seconds. Throws a ValueError if the input
    is not valid.
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

        # Retrieve length, title, broadcast date, and the channel of the VOD
        vod_info = get_vod_info(vod_id)
        self.length = vod_info['length']
        self.title = vod_id['title']
        self.channel = vod_info['channel']
        self.date = vod_id['date']

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
