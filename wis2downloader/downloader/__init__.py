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
from wis2downloader.utils.config import CONFIG
from wis2downloader.celery_app import app as celery_app_instance

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
        self.http = urllib3.PoolManager(
            timeout=timeout,
            maxsize=10,  # Max connections per host
            block=False  # Don't block when pool is full
        )
        self.queue = queue
        self.basepath = Path(basepath)
        self.min_free_space = min_free_space * 1073741824  # GBytes
        self.status = "ready"
        
        # Load configuration settings
        
        # Check if Celery is enabled and set up the flags accordingly
        self.use_celery = CONFIG.get('use_celery', False)
        if self.use_celery:
            # Create a dedicated Celery app instance for this worker
            self.celery_app = celery_app_instance
        
        # Set up directories and paths
        self.basepath = self.basepath.resolve()
        if not self.basepath.is_dir():
            raise ValueError(f"Base path {self.basepath} is not a valid directory.")
        
        # Set up directories for saving downloaded files
        self.save_bufr = CONFIG.get('save_bufr', True)
        self.save_geojson = CONFIG.get('save_geojson', False)
        self.download_dir = Path(CONFIG.get('download_dir', self.basepath / 'downloads')).resolve()
        self.geojson_dir = Path(CONFIG.get('geojson_dir', self.basepath / 'geojson')).resolve()
        
        if not self.download_dir.is_dir():
            self.download_dir.mkdir(parents=True, exist_ok=True)

        if self.save_geojson:
            if not self.use_celery:
                raise ValueError("save_geojson is true, but use_celery is false. Celery is required for GeoJSON conversion.")
            if not self.geojson_dir.is_dir():
                self.geojson_dir.mkdir(parents=True, exist_ok=True)
            
        
        self.bufr2geojson_path = "bufr2geojson"
        if self.save_geojson and not self.check_geojson_conversion():
            raise ValueError("GeoJSON conversion is enabled but the configuration is invalid. "
                             "Please check the 'geojson_dir' setting in your configuration file."
                             "Also make sure that Celery is enabled if you want to use GeoJSON conversion.")
        
        self.bounds = CONFIG.get('bounds', None)
        if self.bounds is None:
            self.check_bounds = False
        else:
            self.check_bounds = True
            self.bounds = {
                'lat_min': self.bounds.get('lat_min', -90),
                'lat_max': self.bounds.get('lat_max', 90),
                'lon_min': self.bounds.get('lon_min', -180),
                'lon_max': self.bounds.get('lon_max', 180)
            }
            
            # Check if bounds are valid
            if not self.check_bounds_validity():
                raise ValueError("Invalid bounds configuration in the config file.")
            
        # Post request configuration
        self.post_enabled = CONFIG.get('post_enabled', False)
        if self.post_enabled:
            self.post_config = CONFIG.get('post_config', {})
            if not self.post_config:
                raise ValueError("Post configuration is enabled but no post_config is provided in the configuration file.")
            is_valid, message = self.validate_post_config(self.post_config)
            if not is_valid:
                raise ValueError(f"Post configuration validation failed: {message}")
            self.post_body_type = self.post_config.get('post_body_type', 'json')

    def start(self) -> None:
        LOGGER.info("Starting download worker")
        while not stop_event.is_set():
            # First get the job from the queue
            job = self.queue.dequeue()
            if not job:
                self.queue.task_done()
                continue
            
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
        """
        Processes a single job.
        If Celery is enabled, it dispatches the download task.
        Otherwise, it performs the download directly.
        """
        data_id = job.get('payload', {}).get('properties', {}).get('data_id', 'Unknown Job')

        if self.check_bounds and not self.is_job_within_bounds(job):
            LOGGER.info(f"Skipping {data_id} as it is outside the defined bounds.")
            return

        # --- Gather all necessary data for dispatching or local processing ---
        _url, update, media_type, expected_size = self.get_download_url(job)

        if _url is None:
            LOGGER.warning(f"No download link found in job for {data_id}")
            return

        expected_hash, hash_function = self.get_hash_info(job)
        # Get the hash method NAME (string) for Celery serialization
        hash_method = job.get('payload', {}).get(
            'properties', {}).get('integrity', {}).get('method')

        yyyy, mm, dd = get_todays_date()
        # Use the configured self.download_dir for consistency
        output_dir = self.download_dir / yyyy / mm / dd / job.get("target", ".")

        filename, _ = self.extract_filename(_url)
        filename += '.' + map_media_type(media_type)
        target_path = output_dir / filename

        # --- Dispatch to Celery or process locally ---
        if self.use_celery:
            LOGGER.info(f"Dispatching download task to Celery for {data_id}")

            # Prevent re-dispatching if the file exists and is not an update
            if target_path.is_file() and not update:
                LOGGER.info(f"Skipping task for {filename}, already exists.")
                return

            self.celery_app.send_task(
                'download_and_process_data',
                kwargs={
                    'download_url': _url,
                    'target_path': str(target_path),
                    'expected_hash': expected_hash,
                    'hash_method': hash_method,
                    'expected_size': expected_size,
                    'min_free_space': self.min_free_space,
                    'basepath_for_space_check': str(self.basepath),
                    'save_bufr': self.save_bufr,
                    'post_config': self.post_config if self.post_enabled else None,
                    'save_geojson_locally': self.save_geojson,
                    'geojson_storage_path': str(self.geojson_dir) if self.geojson_dir else None,
                    'bufr2geojson_path': self.bufr2geojson_path,
                    'date': (yyyy, mm, dd),
                    'job_details': {
                        'data_id': data_id,
                        'topic': job.get('topic'),
                    }
                }
            )
        else:
            LOGGER.info(f"Processing download locally for {data_id} (Celery disabled).")
            target_path.parent.mkdir(parents=True, exist_ok=True)

            if target_path.is_file() and not update:
                LOGGER.info(f"Skipping download of {filename}, already exists")
                return

            topic, centre_id = self.get_topic_and_centre(job)
            download_start = dt.now()
            response = None
            try:
                response = self.http.request('GET', _url)
                if response.status != 200:
                    LOGGER.error(f"Error downloading {_url}, status: {response.status}")
                    FAILED_DOWNLOADS.labels(topic=topic, centre_id=centre_id).inc(1)
                    return
                filesize = len(response.data)
            except Exception as e:
                LOGGER.error(f"Error downloading {_url}: {e}")
                FAILED_DOWNLOADS.labels(topic=topic, centre_id=centre_id).inc(1)
                return

            if self.min_free_space > 0:
                if self.get_free_space() < self.min_free_space:
                    LOGGER.warning(f"Too little free space, file {data_id} not saved")
                    FAILED_DOWNLOADS.labels(topic=topic, centre_id=centre_id).inc(1)
                    return

            if not self.validate_data(response.data, expected_hash, hash_function, expected_size):
                LOGGER.warning(f"Download {data_id} failed verification, discarding")
                FAILED_DOWNLOADS.labels(topic=topic, centre_id=centre_id).inc(1)
                return

            if self.save_bufr:
                self.save_file(response.data, target_path, filename, filesize, download_start)

            DOWNLOADED_BYTES.labels(topic=topic, centre_id=centre_id, file_type='bufr').inc(filesize)
            DOWNLOADED_FILES.labels(topic=topic, centre_id=centre_id, file_type='bufr').inc(1)

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

    def check_coordinates(self, coordinates) -> bool:
        """
        Checks if coordinates are within the defined bounds from the config.
        """
        if isinstance(coordinates, list) and all(isinstance(coord, list) for coord in coordinates):
            return any(self.check_coordinates(coord) for coord in coordinates)

        if not isinstance(coordinates, list) or len(coordinates) < 2:
            return False
        
        lon, lat = coordinates[0], coordinates[1]
        if not all(isinstance(c, (int, float)) for c in [lon, lat]):
            return False

        return (self.bounds['lat_min'] <= lat <= self.bounds['lat_max'] and
                self.bounds['lon_min'] <= lon <= self.bounds['lon_max'])
        
    def check_bounds_validity(self) -> bool:
        """
        Checks if the bounds defined in the config are valid.
        Returns True if valid, False otherwise.
        """
        if not self.check_bounds:
            return True
        
        lat_min = self.bounds['lat_min']
        lat_max = self.bounds['lat_max']
        lon_min = self.bounds['lon_min']
        lon_max = self.bounds['lon_max']
        

        return (lat_min <= lat_max and lon_min <= lon_max and
                -90 <= lat_min <= 90 and -90 <= lat_max <= 90 and
                -180 <= lon_min <= 180 and -180 <= lon_max <= 180)
        
    def is_job_within_bounds(self, job) -> bool:
        """
        Checks if the job's coordinates are present and within the defined bounds.
        Returns False if coordinates are missing or out of bounds.
        """
        geometry = job.get('payload', {}).get('geometry')
        
        if not geometry:
            data_id = job.get('payload', {}).get('properties', {}).get('data_id', 'Unknown')
            LOGGER.info(f"Skipping {data_id}: No geometry found in payload.")
            return False

        coordinates = geometry.get('coordinates')
        if not coordinates:
            data_id = job.get('payload', {}).get('properties', {}).get('data_id', 'Unknown')
            LOGGER.info(f"Skipping {data_id}: No coordinates found to check against bounds.")
            return False

        return self.check_coordinates(coordinates)

    @staticmethod
    def validate_and_get_executable_path(path_from_config: str) -> str:
        """
        Validates the user-provided path for an executable.

        Args:
            path_from_config: The path or name of the executable to validate.

        Returns:
            The absolute path to the executable as a string.

        Raises:
            ValueError: If the path is not provided.
            FileNotFoundError: If the executable cannot be found.
        """
        if not path_from_config:
            raise ValueError("'bufr2geojson_path' must be set in config when celery is enabled.")
        absolute_path = shutil.which(path_from_config)
        LOGGER.debug(f"Absolute path for '{path_from_config}': {absolute_path}")
        if absolute_path is None:
            raise FileNotFoundError(
                f"The executable could not be found for '{path_from_config}'. "
                "Please ensure it's installed and the path in your config is correct."
            )
        return absolute_path
    
    def check_geojson_conversion(self) -> bool:
        """
        Check if the geojson conversion is enabled and the path to bufr2geojson is valid.
        """
        if not self.use_celery:
            LOGGER.warning("Celery is not enabled, skipping GeoJSON conversion check.")
            return False
        if not self.geojson_dir:
            LOGGER.warning("GeoJSON directory is not set.")
            return False
        if not self.bufr2geojson_path:
            LOGGER.warning("Path to bufr2geojson is not set.")
            return False
        try:
            self.validate_and_get_executable_path(self.bufr2geojson_path)
        except (ValueError, FileNotFoundError) as e:
            LOGGER.warning(f"bufr2geojson executable not found at {self.bufr2geojson_path}: {e}")
            return False
        return True

    @staticmethod
    def validate_post_config(config: dict) -> tuple[bool, str]:
        """
        Validates the post configuration dictionary to ensure it's proper.

        Args:
            config: The configuration dictionary to validate.

        Returns:
            A tuple containing:
            - A boolean (True if valid, False otherwise).
            - A string message explaining the result.
        """
        # Check for mandatory base keys
        required_keys = ["post_url", "post_body_type"]
        for key in required_keys:
            if key not in config:
                return False, f"Validation failed: Missing required key '{key}'"

        # Validate the 'post_body_type' value
        post_type = config["post_body_type"]
        if post_type not in ["json", "binary"]:
            return False, f"Validation failed: 'post_body_type' must be 'json' or 'binary', not '{post_type}'"

        # Perform conditional validation based on the post type
        if post_type == "binary":
            # If posting a binary file, 'binary_content_type' is required
            if "binary_content_type" not in config:
                return False, "Validation failed: Missing 'binary_content_type' for post_body_type 'binary'"
            if not isinstance(config["binary_content_type"], str):
                return False, "Validation failed: 'binary_content_type' must be a string."

        # Validate the types of optional keys if they exist
        if "post_headers" in config and not isinstance(config["post_headers"], dict):
            return False, "Validation failed: 'post_headers' must be a dictionary."

        if "post_timeout" in config and not isinstance(config["post_timeout"], (int, float)):
            return False, "Validation failed: 'post_timeout' must be a number."

        # If all checks pass, the configuration is valid
        return True, "Validation successful: Configuration is valid."

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

    def __del__(self):
        """
        Cleanup resources when worker is destroyed.

        This prevents memory leaks by explicitly clearing the urllib3 connection pool
        when the DownloadWorker instance is garbage collected.
        """
        if hasattr(self, 'http'):
            try:
                self.http.clear()
                LOGGER.debug("DownloadWorker: HTTP connection pool cleared")
            except Exception as e:
                LOGGER.warning(f"Error clearing HTTP pool in DownloadWorker cleanup: {e}")