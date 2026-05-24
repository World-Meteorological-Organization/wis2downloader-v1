import json
import tempfile
import pathlib
import urllib3
import hashlib
import base64
import shutil
from datetime import datetime as dt
from celery import shared_task
from wis2downloader.log import LOGGER
from wis2downloader.utils.validate_celery_tasks import post_data
from wis2downloader.metrics import (DOWNLOADED_BYTES, DOWNLOADED_FILES, FAILED_DOWNLOADS)
from wis2downloader.downloader import VerificationMethods

# Import the bufr2geojson transform function
try:
    from bufr2geojson.data import transform as bufr2geojson_transform
except ImportError:
    # This will be used to log an error inside the task
    bufr2geojson_transform = None


# Singleton HTTP pool to prevent memory leaks from creating multiple PoolManager instances
_http_pool = None


def get_http_pool():
    """
    Get or create a singleton HTTP pool for all Celery tasks.

    This prevents memory leaks by reusing a single PoolManager instance
    across all task executions instead of creating a new one per task.
    """
    global _http_pool
    if _http_pool is None:
        timeout = urllib3.Timeout(connect=5.0, read=30.0)
        _http_pool = urllib3.PoolManager(
            timeout=timeout,
            maxsize=10,  # Max connections per host
            block=False  # Don't block when pool is full, raise exception instead
        )
    return _http_pool


@shared_task(bind=True, name='download_and_process_data', ignore_result=True)
def download_and_process_data(self, download_url: str, target_path: str, expected_hash: str,
                              hash_method: str, expected_size: int, save_bufr: bool,
                              min_free_space: int, basepath_for_space_check: str,
                              job_details: dict, **kwargs):
    """
    Celery task to download, validate, save, and then process BUFR data.
    
    This single task handles the entire data acquisition and processing pipeline
    when Celery is enabled.
    """
    data_id = job_details.get('data_id', 'Unknown')
    topic = job_details.get('topic')
    centre_id = topic.split('/')[3] if topic else 'unknown'
    
    LOGGER.info(f"[Celery Task] Starting download for: {data_id} from {download_url}")

    # 1. Download the file from the provided URL
    # Use singleton HTTP pool to prevent memory leaks
    http = get_http_pool()
    try:
        response = http.request('GET', download_url)
        if response.status != 200:
            LOGGER.error(f"[Celery Task] Download failed for {data_id}. Status: {response.status}")
            FAILED_DOWNLOADS.labels(topic=topic, centre_id=centre_id).inc(1)
            return
        bufr_content_bytes = response.data
        filesize = len(bufr_content_bytes)
    except Exception as e:
        LOGGER.error(f"[Celery Task] Exception during download for {data_id}: {e}")
        FAILED_DOWNLOADS.labels(topic=topic, centre_id=centre_id).inc(1)
        return

    # 2. Check for sufficient disk space before saving
    if min_free_space > 0:
        _, _, free = shutil.disk_usage(basepath_for_space_check)
        if free < min_free_space:
            LOGGER.warning(f"[Celery Task] Low disk space ({free} bytes). File {data_id} will not be saved.")
            FAILED_DOWNLOADS.labels(topic=topic, centre_id=centre_id).inc(1)
            return

    # 3. Validate the downloaded data against the expected hash and size
    hash_function = None
    if hash_method and hash_method in VerificationMethods._member_names_:
        hash_function = getattr(hashlib, VerificationMethods[hash_method].value, None)

    # This validation logic is moved here from the DownloadWorker
    def validate_data(data, expected_h, h_function, expected_s):
        if None in (expected_h, h_function, expected_s):
            return True  # No validation criteria provided, so pass
        try:
            hash_value = h_function(data).digest()
            hash_value = base64.b64encode(hash_value).decode()
        except Exception as e:
            LOGGER.error(f"[Celery Task] Hash calculation failed for {data_id}: {e}")
            return False
        if (hash_value != expected_h) or (len(data) != expected_s):
            LOGGER.warning(f"[Celery Task] Validation failed for {data_id}. Hash or size mismatch.")
            return False
        return True

    if not validate_data(bufr_content_bytes, expected_hash, hash_function, expected_size):
        FAILED_DOWNLOADS.labels(topic=topic, centre_id=centre_id).inc(1)
        return
        
    LOGGER.info(f"[Celery Task] Data for {data_id} validated successfully.")

    # 4. Save the raw BUFR file to disk if configured to do so
    if save_bufr:
        target = pathlib.Path(target_path)
        target.parent.mkdir(parents=True, exist_ok=True)
        try:
            target.write_bytes(bufr_content_bytes)
            LOGGER.info(f"[Celery Task] Downloaded {target.name} ({filesize} bytes) to {target}")
        except Exception as e:
            LOGGER.error(f"[Celery Task] Error saving to disk: {target}. Error: {e}")
            FAILED_DOWNLOADS.labels(topic=topic, centre_id=centre_id).inc(1)
            return

    # 5. Increment success metrics for the download
    DOWNLOADED_FILES.labels(topic=topic, centre_id=centre_id, file_type='bufr').inc(1)
    DOWNLOADED_BYTES.labels(topic=topic, centre_id=centre_id, file_type='bufr').inc(filesize)
    
    # --- Integration of original post-processing logic ---
    LOGGER.info(f"[Celery Task] Starting post-processing for {data_id}")

    # Extract post-processing arguments from kwargs
    post_config = kwargs.get('post_config')
    save_geojson_locally = kwargs.get('save_geojson_locally', False)
    geojson_storage_path = kwargs.get('geojson_storage_path')
    date = kwargs.get('date')

    # Post the raw binary data if configured
    if post_config and post_config.get("post_body_type") == "binary":
        LOGGER.info(f"[Celery Task] Posting raw BUFR data for {data_id}.")
        post_data(config=post_config, raw_bytes=bufr_content_bytes)

    # Early exit if no GeoJSON processing is required
    if not save_geojson_locally and (not post_config or post_config.get("post_body_type") != "json"):
        LOGGER.info(f"[Celery Task] No GeoJSON action required for {data_id}. Task finished.")
        return
    
    if not bufr2geojson_transform:
        LOGGER.error(f"[Celery Task] Cannot process {data_id}: bufr2geojson library not installed. "
                     "Install with 'pip install wis2downloader[redis]' or '[rabbitmq]'.")
        return

    # Convert BUFR to GeoJSON using a temporary directory
    try:
        # This function returns a dict like: {'file1.json': {...}, 'file2.json': {...}}
        geojson_outputs = bufr2geojson_transform(bufr_content_bytes)

        if not geojson_outputs:
            LOGGER.warning(f"[Celery Task] bufr2geojson ran for {data_id} but produced no output.")
            return

        # Loop through the dictionary of results
        for file_name, geojson_data in geojson_outputs.items():
            
            # Save the resulting GeoJSON file locally if requested
            if save_geojson_locally and geojson_storage_path:
                storage_path = pathlib.Path(geojson_storage_path)
                yyyy, mm, dd = date if date else (None, None, None)
                if yyyy and mm and dd:
                    storage_path = storage_path / yyyy / mm / dd / data_id
                else:
                    storage_path = storage_path / data_id
                storage_path.mkdir(parents=True, exist_ok=True)
                
                output_file = storage_path / file_name  # Use the file_name from the dict
                try:
                    with open(output_file, 'w') as f_out:
                        json.dump(geojson_data, f_out, indent=4)
                    LOGGER.info(f"[Celery Task] Saved GeoJSON for {data_id} to {output_file}")
                except Exception as e:
                    LOGGER.error(f"[Celery Task] Failed to save GeoJSON to {output_file}: {e}")

            # Post the GeoJSON data if requested
            if post_config and post_config.get("post_body_type") == "json":
                LOGGER.info(f"[Celery Task] Posting GeoJSON from {file_name} for {data_id}.")
                post_data(config=post_config, json_payload=geojson_data)

    except Exception as e:
        LOGGER.error(f"[Celery Task] bufr2geojson (Python library) failed for {data_id}: {e}", exc_info=True)

    LOGGER.info(f"[Celery Task] Task fully completed for data_id: {data_id}")
