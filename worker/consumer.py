"""
Consumes {runId, pipelineId, bucket, specKey} pointer messages from the
"dotwin.pipeline.runs" queue (bound to the "dotwin.runs" exchange with
routing key "dotwin.run" — see rabbitmq/definitions.json), fetches the full
job spec from MinIO at specKey, and runs the pipeline it describes.

This is the piece that was missing: the frontend publishes the job pointer
and then just waits for MQTT events/result — nothing was consuming the
queue, so it sat there forever ("Queued on RabbitMQ..." and never moving).
"""

import json
import os
import time
import traceback

import pika

import storage
import mqtt_report as mq
from pipeline_runner import run_pipeline

RABBITMQ_HOST = os.environ.get("RABBITMQ_HOST", "rabbitmq")
RABBITMQ_PORT = int(os.environ.get("RABBITMQ_PORT", "5672"))
RABBITMQ_USER = os.environ.get("RABBITMQ_USER", "dotwin")
RABBITMQ_PASSWORD = os.environ.get("RABBITMQ_PASSWORD", "dotwin12345")
QUEUE_NAME = os.environ.get("QUEUE_NAME", "dotwin.pipeline.runs")


def handle_message(ch, method, properties, body):
    try:
        pointer = json.loads(body.decode("utf-8"))
    except ValueError as e:
        print(f"[worker] bad message body, dropping: {e}")
        ch.basic_ack(delivery_tag=method.delivery_tag)
        return

    run_id = pointer.get("runId")
    pipeline_id = pointer.get("pipelineId")
    spec_key = pointer.get("specKey")
    print(f"[worker] got job: run={run_id} pipeline={pipeline_id} spec={spec_key}")

    # ack immediately — a long-running training job holding an unacked
    # delivery can trip RabbitMQ's consumer timeout and get redelivered
    # mid-run, causing it to be picked up twice. Progress/failure is
    # reported over MQTT regardless of what happens next.
    ch.basic_ack(delivery_tag=method.delivery_tag)

    try:
        job = storage.get_json(spec_key)
        config = job["config"]
        run_pipeline(run_id, pipeline_id, config)
        print(f"[worker] run {run_id} finished OK")
    except Exception as e:  # noqa: BLE001
        print(f"[worker] run {run_id} FAILED: {e}")
        traceback.print_exc()
        if run_id and pipeline_id:
            mq.emit_error(pipeline_id, run_id, str(e))


def main():
    # Fail loud and immediately if MQTT isn't reachable — otherwise every
    # single node_start/node_done event silently no-ops (mqtt_report.py only
    # prints on failure) and the frontend just looks "stuck", exactly like a
    # RabbitMQ-side problem would. Checking this once at boot makes the two
    # failure modes distinguishable in `docker compose logs worker`.
    try:
        import paho.mqtt.publish as _mqp
        _mqp.single("dotwin/worker/selftest", payload="ok", hostname=mq.MQTT_HOST, port=mq.MQTT_PORT)
        print(f"[worker] MQTT reachable at {mq.MQTT_HOST}:{mq.MQTT_PORT}")
    except Exception as e:  # noqa: BLE001
        print(f"[worker] WARNING: could not reach MQTT at {mq.MQTT_HOST}:{mq.MQTT_PORT} — {e}")
        print("[worker] every step will silently fail to report progress until this is fixed.")

    creds = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASSWORD)
    params = pika.ConnectionParameters(host=RABBITMQ_HOST, port=RABBITMQ_PORT, credentials=creds, heartbeat=30)

    while True:
        try:
            connection = pika.BlockingConnection(params)
            channel = connection.channel()
            channel.queue_declare(queue=QUEUE_NAME, durable=True)
            channel.basic_qos(prefetch_count=1)  # one job at a time per worker instance
            channel.basic_consume(queue=QUEUE_NAME, on_message_callback=handle_message)
            print(f"[worker] listening on queue '{QUEUE_NAME}' at {RABBITMQ_HOST}:{RABBITMQ_PORT}...")
            channel.start_consuming()
        except (pika.exceptions.AMQPConnectionError, pika.exceptions.StreamLostError) as e:
            print(f"[worker] RabbitMQ connection lost/unavailable ({e}), retrying in 5s...")
            time.sleep(5)
        except KeyboardInterrupt:
            break


if __name__ == "__main__":
    main()
