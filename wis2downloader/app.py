import json
from pathlib import Path
import threading
from urllib.parse import unquote
from uuid import uuid4
import yaml

from flask import Flask, request, jsonify, Response, render_template, abort, url_for

from flask_cors import CORS
from prometheus_client import generate_latest, REGISTRY

from wis2downloader import stop_event
from wis2downloader.downloader import DownloadWorker
from wis2downloader.log import LOGGER, setup_logger
from wis2downloader.queue import SimpleQueue
from wis2downloader.subscriber import MQTTSubscriber
from wis2downloader.utils.validate_target import validate_target
from wis2downloader.utils.validate_topic import validate_topic
from wis2downloader.utils.config import load_config

CONFIG = load_config()


setup_logger(loglevel=CONFIG['log_level'],
             save=CONFIG['save_logs'],
             log_path=CONFIG['log_path'])

# Create the queue with configurable max size to prevent unbounded memory growth
jobQ = SimpleQueue(maxsize=CONFIG.get('queue_maxsize', 10000))

# Now set up the different threads
# 2) download workers
# 3) subscriber

# Start workers to process the jobs from the queue
worker_threads = []

for idx in range(CONFIG['download_workers']):
    worker = DownloadWorker(jobQ, CONFIG['download_dir'],
                            CONFIG['min_free_space'])
    worker_thread = threading.Thread(target=worker.start, daemon=True)
    worker_threads.append(worker_thread)
    worker_thread.start()

# Now create the MQTT subscriber
mqtt_session = Path(CONFIG['mqtt_session_info'])
if mqtt_session is None:
    mqtt_session = ".mqtt_session.json"

mqtt_session = Path(mqtt_session)
if mqtt_session.is_file():
    with open(mqtt_session) as fh:
        session_info = json.load(fh)
        if session_info.get('client_id') is None:
            session_info['client_id'] = str(uuid4())
else:
    session_info = {
        'topics': {},
        'client_id': str(uuid4())
    }

LOGGER.info(session_info)

subscriber = MQTTSubscriber(
    CONFIG['broker_hostname'],
    CONFIG['broker_port'],
    CONFIG['broker_username'],
    CONFIG['broker_password'],
    CONFIG['broker_protocol'],
    jobQ,
    session_info['client_id']
)

# Now spawn subscriber as thread
mqtt_thread = threading.Thread(target=subscriber.start, daemon=True)
mqtt_thread.start()

# Add default subscriptions
for topic, target in session_info['topics'].items():
    if CONFIG['validate_topics']:
        is_topic_valid, _ = validate_topic(topic)
    else:
        is_topic_valid = True

    if not is_topic_valid:
        LOGGER.warning(
            "Invalid topic in default config, please check config file")
        continue

    is_target_valid, _ = validate_target(target)
    if not is_target_valid:
        LOGGER.warning(
            "Invalid target in default config, please check config file")
        continue

    subscriber.add_subscription(topic, target)

# Provided the app.run() call is blocking, the following code will only
# be executed when the Flask app is stopped


def shutdown():
    LOGGER.info("Shutting down")

    # Stop the subscriber first
    subscriber.stop()

    # Signal all other threads to stop
    stop_event.set()

    # now join mqtt thread
    mqtt_thread.join()
    LOGGER.info("Subscriber thread stopped")

    for worker in worker_threads:
        LOGGER.info("Shutting down worker threads")
        # If download worker is blocked waiting for a job, send one
        if jobQ.size() == 0:
            jobQ.enqueue({'shutdown': True})
        worker.join()


# Create and run Flask instance

app = Flask(__name__, instance_relative_config=True)
CORS(app)

# Define routes


@app.route('/metrics')
def expose_metrics():
    """
    Expose the Prometheus metrics to be scraped.
    """
    return Response(generate_latest(REGISTRY), mimetype="text/plain")


@app.get('/subscriptions')
def list_subscriptions():
    subs = subscriber.list_subscriptions()
    return jsonify(subs)


@app.post('/subscriptions')
def add_subscription():
    # Topic validation
    if not request.is_json:
        abort(400, "Invalid input")

    data = request.json
    topic = unquote(data.get("topic"))
    LOGGER.info(f"Subscribing to {topic}")
    if CONFIG['validate_topics']:
        is_topic_valid, msg = validate_topic(topic)
    else:
        is_topic_valid = True
        msg = ''

    if not is_topic_valid:
        abort(400, f"Invalid input ({msg})")

    # Target validation
    target = data.get('target')
    if target in (None, "$TOPIC"):
        target = "$TOPIC"

    is_target_valid, msg = validate_target(target)

    if not is_target_valid:
        abort(400, f"Invalid input ({msg})")

    try:
        subs = subscriber.add_subscription(topic, target)
    except Exception as e:
        abort(500, f"Internal server error: {e}")

    session_info['topics'][topic] = target

    try:
        with open(CONFIG['mqtt_session_info'], 'w') as fh:
            json.dump(session_info, fh)
    except Exception as e:
        abort(500, f"Internal server error: {e}")

    response = jsonify(subs[topic])
    response.status_code = 201
    response.headers['Location'] = url_for('get_subscription', topic=topic)

    return response


@app.get('/subscriptions/<path:topic>')
def get_subscription(topic):
    # Topic validation
    topic = unquote(topic)
    if CONFIG['validate_topics']:
        is_topic_valid, msg = validate_topic(topic)
    else:
        is_topic_valid = True
        msg = None

    if not is_topic_valid:
        abort(400, f"Invalid input ({msg})")

    if topic not in subscriber.active_subscriptions:
        abort(404, "Subscription not found")

    return jsonify(subscriber.active_subscriptions[topic])


@app.delete('/subscriptions/<path:topic>')
def delete_subscription(topic):
    topic = unquote(topic)
    # Topic validation
    if CONFIG['validate_topics']:
        is_topic_valid, msg = validate_topic(topic)
    else:
        is_topic_valid = True
        msg = None

    if not is_topic_valid:
        abort(400, f"Invalid input ({msg})")

    if topic not in subscriber.active_subscriptions:
        abort(404, "Subscription not found")

    subs = subscriber.delete_subscription(topic)

    del session_info['topics'][topic]

    with open(CONFIG['mqtt_session_info'], 'w') as fh:
        json.dump(session_info, fh)

    return Response(response=json.dumps(subs), status=200,
                    mimetype="application/json")


@app.route('/')
@app.route('/swagger')
def render_swagger():
    return render_template('swagger.html', )


@app.route('/openapi')
def fetch_openapi():
    p = Path(app.root_path) / 'static' / 'openapi.yaml'
    with open(p) as fh:
        openapi_doc = yaml.safe_load(fh)
    openapi_doc['servers'] = [
        {"url": CONFIG['base_url']}
    ]
    return jsonify(openapi_doc)


@app.route('/health')
def health_check():
    return Response(response=json.dumps({'status': 'healthy'}), status=200,
                    mimetype="application/json")


def run():
    app.run(debug=True, host=CONFIG['flask_host'],
            port=CONFIG['flask_port'], use_reloader=False)
    shutdown()
