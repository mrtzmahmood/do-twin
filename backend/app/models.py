from pydantic import BaseModel
from typing import Any, Dict, Optional

class PutObjectBody(BaseModel):
    key: str
    content: str
    contentType: str = "application/json"

class PublishDashboardBody(BaseModel):
    userKey: str
    projectId: Optional[str] = None
    dashboardId: Optional[str] = None
    snapshot: Dict[str, Any]

class PublishBody(BaseModel):
    exchange: str
    routingKey: str
    payload: str
    vhost: str | None = None


class MqttPublishBody(BaseModel):
    topic: str
    payload: str
    qos: int = 0


class DbConnectionBody(BaseModel):
    engine: str          # "postgres" | "mysql"
    host: str
    port: int
    user: str
    password: str
    database: str


class DbQueryBody(DbConnectionBody):
    query: str
    limit: int = 500
