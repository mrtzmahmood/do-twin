from sqlalchemy import create_engine, text

# Connection details are supplied per-request by the pipeline's SQL Source
# node (host/port/user/password/database) — this backend has no fixed DB of
# its own to query here; the compose file's postgres/mysql services are just
# convenient sample targets to point that node at.

_DRIVERS = {
    "postgres": "postgresql+psycopg2",
    "mysql": "mysql+pymysql",
}


def _build_url(engine: str, host: str, port: int, user: str, password: str, database: str) -> str:
    driver = _DRIVERS.get(engine)
    if not driver:
        raise ValueError(f'Unsupported engine "{engine}" — expected "postgres" or "mysql".')
    return f"{driver}://{user}:{password}@{host}:{port}/{database}"


def test_connection(engine: str, host: str, port: int, user: str, password: str, database: str) -> dict:
    try:
        url = _build_url(engine, host, port, user, password, database)
        eng = create_engine(url, connect_args={"connect_timeout": 5} if engine == "postgres" else {})
        with eng.connect() as conn:
            conn.execute(text("SELECT 1"))
        return {"ok": True}
    except Exception as e:  # noqa: BLE001
        return {"ok": False, "message": str(e)}


def run_query(engine: str, host: str, port: int, user: str, password: str, database: str, query: str, limit: int = 500) -> dict:
    stripped = query.strip().rstrip(";")
    if not stripped.lower().startswith("select"):
        return {"ok": False, "message": "Only SELECT queries are allowed from the pipeline's SQL Source node."}
    try:
        url = _build_url(engine, host, port, user, password, database)
        eng = create_engine(url)
        with eng.connect() as conn:
            result = conn.execute(text(f"SELECT * FROM ({stripped}) AS q LIMIT :lim"), {"lim": limit})
            cols = list(result.keys())
            rows = [dict(zip(cols, row)) for row in result.fetchall()]
        return {"ok": True, "columns": cols, "rows": rows}
    except Exception as e:  # noqa: BLE001
        return {"ok": False, "message": str(e)}
