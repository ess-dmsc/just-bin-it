import json
import os.path
import signal
import sys
import time
import uuid
from subprocess import Popen

import pytest
from compose.cli.main import TopLevelCommand, project_from_options
from confluent_kafka import OFFSET_END, Consumer, Producer, TopicPartition
from confluent_kafka.admin import AdminClient
from integration_settings import BROKERS, KAFKA_MANAGED_EXTERNALLY

common_options = {
    "--no-deps": False,
    "--always-recreate-deps": False,
    "--scale": "",
    "--abort-on-container-exit": False,
    "SERVICE": "",
    "--remove-orphans": False,
    "--no-recreate": True,
    "--force-recreate": False,
    "--no-build": False,
    "--no-color": False,
    "--rmi": "none",
    "--volumes": True,  # Remove volumes when docker-compose down (don't persist kafka and zk data)
    "--follow": False,
    "--timestamps": False,
    "--tail": "all",
    "--detach": True,
    "--build": False,
    "--no-log-prefix": False,
}

WAIT_FOR_DEBUGGER_ATTACH = "--wait-to-attach-debugger"

CMD_TOPIC = "hist_commands"
RESPONSE_TOPIC = "hist_responses"
POLL_INTERVAL_S = 0.05


def pytest_addoption(parser):
    parser.addoption(
        WAIT_FOR_DEBUGGER_ATTACH,
        type=bool,
        action="store",
        default=False,
        help="Use this flag to cause the integration tests to prompt you to attach a debugger to the just-bin-it process",
    )


def stop_kafka(docker_cmd, docker_options):
    if docker_cmd is not None and docker_options is not None:
        docker_cmd.down(docker_options)


def wait_until_kafka_ready(docker_cmd=None, docker_options=None):
    print("Waiting for Kafka broker to be ready for integration tests...")
    conf = {"bootstrap.servers": ",".join(BROKERS)}
    producer = Producer(conf)
    kafka_ready = False

    def delivery_callback(err, msg):
        nonlocal n_polls
        nonlocal kafka_ready
        if not err:
            print("Kafka is ready!")
            kafka_ready = True

    n_polls = 0
    while n_polls < 10 and not kafka_ready:
        producer.produce(
            "waitUntilUp", value="Test message", on_delivery=delivery_callback
        )
        producer.poll(10)
        n_polls += 1

    if not kafka_ready:
        stop_kafka(docker_cmd, docker_options)
        raise Exception("Kafka broker was not ready after 100 seconds, aborting tests.")

    client = AdminClient(conf)
    topics_ready = False

    deadline = time.monotonic() + 60
    while time.monotonic() < deadline and not topics_ready:
        topics = set(client.list_topics().topics.keys())
        topics_needed = [CMD_TOPIC, RESPONSE_TOPIC]
        present = [t in topics for t in topics_needed]
        if all(present):
            topics_ready = True
            print("Topics are ready!", flush=True)
            break
        time.sleep(0.5)

    if not topics_ready:
        stop_kafka(docker_cmd, docker_options)
        raise Exception("Kafka topics were not ready after 60 seconds, aborting tests.")


def wait_until_just_bin_it_ready(proc, timeout=15):
    conf = {
        "bootstrap.servers": ",".join(BROKERS),
        "group.id": uuid.uuid4(),
        "auto.offset.reset": "latest",
    }
    consumer = Consumer(conf)
    producer = Producer({"bootstrap.servers": ",".join(BROKERS)})
    response_metadata = consumer.list_topics(RESPONSE_TOPIC, timeout=timeout)
    topic_partitions = [
        TopicPartition(RESPONSE_TOPIC, partition.id, OFFSET_END)
        for partition in response_metadata.topics[RESPONSE_TOPIC].partitions.values()
    ]
    consumer.assign(topic_partitions)
    msg_id = f"startup-{uuid.uuid4()}"
    message = json.dumps({"cmd": "not a valid command", "msg_id": msg_id}).encode()
    deadline = time.monotonic() + timeout
    next_send = 0

    try:
        while time.monotonic() < deadline:
            if proc.poll() is not None:
                raise Exception("just-bin-it process exited during startup")

            now = time.monotonic()
            if now >= next_send:
                producer.produce(CMD_TOPIC, message)
                producer.flush(1)
                next_send = now + 0.5

            msg = consumer.poll(POLL_INTERVAL_S)
            if msg is None:
                continue
            if msg.error():
                raise Exception(msg.error())

            response = json.loads(msg.value())
            if response.get("msg_id") == msg_id and response.get("response") == "ERR":
                return
    finally:
        consumer.close()

    raise Exception("just-bin-it was not ready before timeout")


@pytest.fixture(scope="session", autouse=True)
def start_kafka(request):
    print("Starting zookeeper and kafka", flush=True)

    if KAFKA_MANAGED_EXTERNALLY:
        print("Kafka is managed externally", flush=True)
        wait_until_kafka_ready()
        return

    options = common_options
    options["--project-name"] = "kafka"
    options["--file"] = ["docker-compose.yml"]
    project = project_from_options(os.path.dirname(__file__), options)
    cmd = TopLevelCommand(project)

    cmd.up(options)
    print("Started kafka containers", flush=True)
    wait_until_kafka_ready(cmd, options)

    def fin():
        print("Stopping zookeeper and kafka", flush=True)
        options["--timeout"] = 30
        options["--project-name"] = "kafka"
        options["--file"] = ["docker-compose.yml"]
        cmd.down(options)

    request.addfinalizer(fin)


@pytest.fixture(scope="module")
def just_bin_it(request):
    print("Started preparing test environment...", flush=True)
    proc = Popen(
        [
            sys.executable,
            "../bin/just-bin-it.py",
            "-b",
            *BROKERS,
            "-t",
            "hist_commands",
            "-rt",
            "hist_responses",
        ]
    )

    wait_until_just_bin_it_ready(proc)

    wait_for_debugger = request.config.getoption(WAIT_FOR_DEBUGGER_ATTACH)

    if wait_for_debugger:
        proc.send_signal(signal.SIGSTOP)
        input(
            f"\n"
            f"Attach a debugger to process id {proc.pid} now if you wish, "
            f"then press enter to continue: "
        )

        proc.send_signal(signal.SIGCONT)

    def fin():
        proc.kill()

    # Using a finalizer rather than yield in the fixture means
    # that the process will be brought down even if tests fail.
    request.addfinalizer(fin)
