from fastapi import APIRouter
from fastapi.concurrency import run_in_threadpool

from app.models import DbConnectionBody, DbQueryBody
from app.services import db_client

router = APIRouter(prefix="/api/db", tags=["db"])


@router.post("/test")
async def test_connection(body: DbConnectionBody):
    return await run_in_threadpool(
        db_client.test_connection, body.engine, body.host, body.port, body.user, body.password, body.database,
    )


@router.post("/query")
async def run_query(body: DbQueryBody):
    return await run_in_threadpool(
        db_client.run_query, body.engine, body.host, body.port, body.user, body.password, body.database, body.query, body.limit,
    )
