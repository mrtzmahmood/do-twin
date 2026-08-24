from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    # ---- MinIO / S3-compatible storage ----
    minio_endpoint: str = "minio:9000"
    minio_access_key: str = "dotwin"
    minio_secret_key: str = "dotwin12345"
    minio_secure: bool = False
    minio_bucket: str = "dotwin"

    # ---- RabbitMQ (HTTP Management API — no AMQP client needed server-side either) ----
    rabbitmq_api_url: str = "http://rabbitmq:15672"
    rabbitmq_user: str = "dotwin"
    rabbitmq_password: str = "dotwin12345"
    rabbitmq_vhost: str = "/"

    # ---- MQTT ----
    mqtt_host: str = "mosquitto"
    mqtt_port: int = 1883
    mqtt_username: str | None = None
    mqtt_password: str | None = None

    # ---- CORS ----
    cors_origins: str = "*"

    class Config:
        env_file = ".env"


settings = Settings()
