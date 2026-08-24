"""
Publishes run progress/results on the SAME topics the DoTwin frontend
subscribes to (see BrokerPanel's wire-protocol comment in the frontend):

    {prefix}/pipelines/{pipelineId}/run/{runId}/events
        {"runId", "nodeId", "type": "node_start"|"node_done"|"node_error", "message"}
    {prefix}/pipelines/{pipelineId}/run/{runId}/result
        {"runId", "type": "done", "resultKey"}   or   {"type": "error", "message"}

Uses paho.mqtt.publish.single() per message (a short-lived connection per
call) rather than one long-lived client — simple, and avoids any surprises
if this ever runs inside something that pickles the calling function (e.g.
a Ray worker), same reasoning as the original script this replaces.
"""

import json
import os
import time

import paho.mqtt.publish as mqtt_publish

MQTT_HOST = os.environ.get("MQTT_HOST", "mosquitto")
MQTT_PORT = int(os.environ.get("MQTT_PORT", "1883"))
TOPIC_PREFIX = os.environ.get("TOPIC_PREFIX", "dotwin")


def _base_topic(pipeline_id: str, run_id: str) -> str:
    return f"{TOPIC_PREFIX}/pipelines/{pipeline_id}/run/{run_id}"


def _publish(topic: str, payload: dict) -> None:
    try:
        mqtt_publish.single(
            topic,
            payload=json.dumps(payload),
            qos=1,
            hostname=MQTT_HOST,
            port=MQTT_PORT,
        )
    except Exception as e:  # noqa: BLE001 — a report failing shouldn't kill the run
        print(f"[mqtt_report] failed to publish to {topic}: {e}")


def emit_event(pipeline_id: str, run_id: str, node_id: str | None, event_type: str, message: str = "") -> None:
    _publish(_base_topic(pipeline_id, run_id) + "/events", {
        "runId": run_id, "nodeId": node_id, "type": event_type, "message": message, "ts": time.time(),
    })


def emit_done(pipeline_id: str, run_id: str, result_key: str) -> None:
    _publish(_base_topic(pipeline_id, run_id) + "/result", {
        "runId": run_id, "type": "done", "resultKey": result_key,
    })


def emit_error(pipeline_id: str, run_id: str, message: str) -> None:
    _publish(_base_topic(pipeline_id, run_id) + "/result", {
        "runId": run_id, "type": "error", "message": message,
    })
