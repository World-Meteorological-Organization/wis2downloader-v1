from abc import ABC, abstractmethod
import urllib3
from urllib.parse import urlsplit
import hashlib
import base64
import os
from datetime import datetime as dt
from pathlib import Path
import enum
import shutil

from wis2downloader import stop_event
from wis2downloader.log import LOGGER
from wis2downloader.queue import BaseQueue
from wis2downloader.metrics import (DOWNLOADED_BYTES, DOWNLOADED_FILES,
                                    FAILED_DOWNLOADS)


class BaseDownloader(ABC):

    @abstractmethod
    def start(self):
        """Start the download worker to
        process messages from the queue indefinitely"""
        pass

    @abstractmethod
    def process_job(self, job):
        """Process a single job from the queue"""
        pass

    @abstractmethod
    def get_topic_and_centre(self, job):
        """Extract the topic and centre id from the job"""
        pass

    @abstractmethod
    def get_hash_info(self, job):
        """Extract the hash value and function from the job
        to be used for verification later"""
        pass

    @abstractmethod
    def get_download_url(self, job):
        """Extract the download url, update status, and
        file type from the job links"""
        pass

    @abstractmethod
    def extract_filename(self, _url):
        """Extract the filename and extension from the download link"""
        pass

    @abstractmethod
    def validate_data(self, data, expected_hash, hash_function, expected_size):
        """Validate the hash and size of the downloaded data against
        the expected values"""
        pass

    @abstractmethod
    def save_file(self, data, target, filename, filesize, download_start):
        """Save the downloaded data to disk"""


def get_todays_date():
    """
    Returns today's date in the format yyyy/mm/dd.
    """
    today = dt.now()
    yyyy = f"{today.year:04}"
    mm = f"{today.month:02}"
    dd = f"{today.day:02}"
    return yyyy, mm, dd


def map_media_type(media_type):
    _map = {
        "application/x-bufr": "bufr",
        "application/octet-stream": "bin",
        "application/xml": "xml",
        "image/jpeg": "jpeg",
        "application/x-grib": "grib",
        "application/grib;edition=2": "grib",
        "text/plain": "txt"
    }

    return _map.get(media_type, 'bin')


class VerificationMethods(enum.Enum):
    sha256 = 'sha256'
    sha384 = 'sha384'
    sha512 = 'sha512'
    sha3_256 = 'sha3_256'
    sha3_384 = 'sha3_384'
    sha3_512 = 'sha3_512'


class DownloadWorker(BaseDownloader):
    def __init__(self, queue: BaseQueue, basepath: str = ".", min_free_space=10):  # noqa
        timeout = urllib3.Timeout(connect=1.0)
        self.http = urllib3.PoolManager(timeout=timeout)
        self.queue = queue
        self.basepath = Path(basepath)
        self.min_free_space = min_free_space * 1073741824  # GBytes
        self.status = "ready"

    def start(self) -> None:
        LOGGER.info("Starting download worker")
        while not stop_event.is_set():
            # First get the job from the queue
            job = self.queue.dequeue()
            if job.get('shutdown', False):
                break

            self.status = "running"
            try:
                self.process_job(job)
            except Exception as e:
                LOGGER.error(e)

            self.status = "ready"
            self.queue.task_done()

    def get_free_space(self):
        total, used, free = shutil.disk_usage(self.basepath)
        return free

    def process_job(self, job) -> None:
        yyyy, mm, dd = get_todays_date()
        output_dir = self.basepath / yyyy / mm / dd

        # Add target to output directory
        output_dir = output_dir / job.get("target", ".")

        # Get information about the job for verification later
        expected_hash, hash_function = self.get_hash_info(job)

        # Get the download url, update status, and file type from the job links
        _url, update, media_type, expected_size = self.get_download_url(job)

        if _url is None:
            LOGGER.warning(f"No download link found in job {job}")
            return

        # map media type to file extension
        file_type = map_media_type(media_type)

        # Global caches can set whatever filename they want, we need to use
        # the data_id for uniqueness. However, this can be unwieldy, hence use
        # hash of data_id
        data_id = job.get('payload', {}).get('properties', {}).get('data_id')
        filename, _ = self.extract_filename(_url)
        filename = filename + '.' + file_type
        target = output_dir / filename
        # Create parent dir if it doesn't exist
        target.parent.mkdir(parents=True, exist_ok=True)

        # Only download if file doesn't exist or is an update
        is_duplicate = target.is_file() and not update
        if is_duplicate:
            LOGGER.info(f"Skipping download of {filename}, already exists")
            return

        # Get information needed for download metric labels
        topic, centre_id = self.get_topic_and_centre(job)

        # Standardise the file type label, defaulting to 'other'
        all_type_labels = ['bufr', 'grib', 'json', 'xml', 'png']
        file_type_label = file_type if file_type in all_type_labels else 'other'

        # Start timer of download time to be logged later
        download_start = dt.now()

        # Download the file
        response = None
        try:
            response = self.http.request('GET', _url)
            # Check the response status code
            if response.status != 200:
                LOGGER.error(f"Error downloading {_url}, received status code: {response.status}")
                # Increment failed download counter
                FAILED_DOWNLOADS.labels(topic=topic, centre_id=centre_id).inc(1)
                return
            # Get the filesize in KB
            filesize = len(response.data)
        except Exception as e:
            LOGGER.error(f"Error downloading {_url}")
            LOGGER.error(e)
            # Increment failed download counter
            FAILED_DOWNLOADS.labels(topic=topic, centre_id=centre_id).inc(1)
            return

        if self.min_free_space > 0:  # only check size if limit set
            free_space = self.get_free_space()
            if free_space < self.min_free_space:
                LOGGER.warning(f"Too little free space, {free_space - filesize} < {self.min_free_space} , file {data_id} not saved")  # noqa
                FAILED_DOWNLOADS.labels(topic=topic, centre_id=centre_id).inc(1)
                return

        if response is None:
            FAILED_DOWNLOADS.labels(topic=topic, centre_id=centre_id).inc(1)
            return

        # Use the hash function to determine whether to save the data
        save_data = self.validate_data(
            response.data, expected_hash, hash_function, expected_size)

        if not save_data:
            LOGGER.warning(f"Download {data_id} failed verification, discarding")  # noqa
            # Increment failed download counter
            FAILED_DOWNLOADS.labels(topic=topic, centre_id=centre_id).inc(1)
            return

        # Now save
        self.save_file(response.data, target, filename,
                       filesize, download_start)

        # Increment metrics
        DOWNLOADED_BYTES.labels(
            topic=topic, centre_id=centre_id,
            file_type=file_type_label).inc(filesize)
        DOWNLOADED_FILES.labels(
            topic=topic, centre_id=centre_id,
            file_type=file_type_label).inc(1)

    def get_topic_and_centre(self, job) -> tuple:
        topic = job.get('topic')
        return topic, topic.split('/')[3]

    def get_hash_info(self, job):
        expected_hash = job.get('payload', {}).get(
            'properties', {}).get('integrity', {}).get('value')
        hash_method = job.get('payload', {}).get(
            'properties', {}).get('integrity', {}).get('method')

        hash_function = None

        # Check if hash method is known using our enumumeration of hash methods
        if hash_method in VerificationMethods._member_names_:
            # get method
            method = VerificationMethods[hash_method].value
            # load and return from the hashlib library
            hash_function = getattr(hashlib, method, None)

        return expected_hash, hash_function

    def get_download_url(self, job) -> tuple:
        links = job.get('payload', {}).get('links', [])
        _url = None
        update = False
        media_type = None
        expected_size = None
        for link in links:
            if link.get('rel') == 'update':
                _url = link.get('href')
                media_type = link.get('type')
                expected_size = link.get('length')
                update = True
                break
            elif link.get('rel') == 'canonical':
                _url = link.get('href')
                media_type = link.get('type')
                expected_size = link.get('length')
                break

        return _url, update, media_type, expected_size

    def extract_filename(self, _url) -> tuple:
        path = urlsplit(_url).path
        filename = os.path.basename(path)
        filename, filename_ext = os.path.splitext(filename)
        return filename, filename_ext

    def validate_data(self, data, expected_hash,
                      hash_function, expected_size) -> bool:
        if None in (expected_hash, hash_function,
                    hash_function):
            return True

        try:
            hash_value = hash_function(data).digest()
            hash_value = base64.b64encode(hash_value).decode()
        except Exception as e:
            LOGGER.error(e)
            return False
        if (hash_value != expected_hash) or (len(data) != expected_size):
            return False

        return True

    def save_file(self, data, target, filename, filesize,
                  download_start) -> None:
        try:
            target.write_bytes(data)
            download_end = dt.now()
            download_time = download_end - download_start
            download_seconds = round(download_time.total_seconds(), 2)
            LOGGER.info(
                f"Downloaded {filename} of size {filesize} bytes in {download_seconds} seconds")  # noqa
        except Exception as e:
            LOGGER.error(f"Error saving to disk: {target}")
            LOGGER.error(e)
