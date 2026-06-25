"""
DoTwin Pipeline Builder - FastAPI Backend
"""
from fastapi import FastAPI, UploadFile, File, HTTPException
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse, JSONResponse
from fastapi.middleware.cors import CORSMiddleware
import uvicorn
import shutil
import os
import json
from pathlib import Path
from datetime import datetime

app = FastAPI(
    title="DoTwin Pipeline Builder API",
    description="Backend for the DoTwin Pipeline Builder application",
    version="1.0.0"
)

# CORS for frontend dev server
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Paths
BASE_DIR = Path(__file__).parent.parent
FRONTEND_DIR = BASE_DIR / "frontend"
UPLOADS_DIR = BASE_DIR / "uploads"
PIPELINES_DIR = BASE_DIR / "data" / "pipelines"

for d in [UPLOADS_DIR, PIPELINES_DIR]:
    d.mkdir(parents=True, exist_ok=True)

# Serve static frontend files
app.mount("/static", StaticFiles(directory=str(FRONTEND_DIR)), name="static")


@app.get("/")
async def root():
    """Serve the main frontend page."""
    index_path = FRONTEND_DIR / "index.html"
    if index_path.exists():
        return FileResponse(str(index_path))
    return JSONResponse({"message": "DoTwin Pipeline Builder API", "docs": "/docs"})


# ─────────────────────────────────────────
#  Pipeline CRUD
# ─────────────────────────────────────────

@app.get("/api/pipelines")
async def list_pipelines():
    """List all saved pipelines."""
    pipelines = []
    for f in sorted(PIPELINES_DIR.glob("*.json")):
        try:
            data = json.loads(f.read_text())
            pipelines.append({
                "id": f.stem,
                "name": data.get("name", f.stem),
                "updated_at": data.get("updated_at", ""),
                "node_count": len(data.get("nodes", [])),
                "edge_count": len(data.get("edges", []))
            })
        except Exception:
            pass
    return {"pipelines": pipelines}


@app.get("/api/pipelines/{pipeline_id}")
async def get_pipeline(pipeline_id: str):
    """Get a specific pipeline by ID."""
    path = PIPELINES_DIR / f"{pipeline_id}.json"
    if not path.exists():
        raise HTTPException(status_code=404, detail="Pipeline not found")
    return json.loads(path.read_text())


@app.post("/api/pipelines")
async def create_pipeline(payload: dict):
    """Create a new pipeline."""
    pipeline_id = payload.get("id") or f"pipeline_{int(datetime.now().timestamp())}"
    payload["id"] = pipeline_id
    payload["created_at"] = datetime.now().isoformat()
    payload["updated_at"] = datetime.now().isoformat()

    path = PIPELINES_DIR / f"{pipeline_id}.json"
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2))
    return {"success": True, "id": pipeline_id, "pipeline": payload}


@app.put("/api/pipelines/{pipeline_id}")
async def update_pipeline(pipeline_id: str, payload: dict):
    """Update an existing pipeline."""
    path = PIPELINES_DIR / f"{pipeline_id}.json"

    existing = {}
    if path.exists():
        existing = json.loads(path.read_text())

    existing.update(payload)
    existing["id"] = pipeline_id
    existing["updated_at"] = datetime.now().isoformat()
    if "created_at" not in existing:
        existing["created_at"] = existing["updated_at"]

    path.write_text(json.dumps(existing, ensure_ascii=False, indent=2))
    return {"success": True, "id": pipeline_id, "pipeline": existing}


@app.delete("/api/pipelines/{pipeline_id}")
async def delete_pipeline(pipeline_id: str):
    """Delete a pipeline."""
    path = PIPELINES_DIR / f"{pipeline_id}.json"
    if not path.exists():
        raise HTTPException(status_code=404, detail="Pipeline not found")
    path.unlink()
    return {"success": True, "id": pipeline_id}


# ─────────────────────────────────────────
#  Node types / component catalog
# ─────────────────────────────────────────

NODE_CATALOG = [
    {"type": "data_source",     "label": "Data Source",      "category": "Input",      "color": "#0E7C6B"},
    {"type": "feature_eng",     "label": "Feature Engineering","category": "Processing", "color": "#2563EB"},
    {"type": "ml_model",        "label": "ML Model",          "category": "Model",      "color": "#7C3AED"},
    {"type": "rl_agent",        "label": "RL Agent",          "category": "Model",      "color": "#DC2626"},
    {"type": "fuzzy_inference", "label": "Fuzzy Inference",   "category": "Model",      "color": "#EA580C"},
    {"type": "anomaly_detect",  "label": "Anomaly Detection", "category": "Processing", "color": "#0891B2"},
    {"type": "pred_maintenance","label": "Predictive Maint.", "category": "Output",     "color": "#16A34A"},
    {"type": "digital_twin",    "label": "Digital Twin",      "category": "Output",     "color": "#CA8A04"},
    {"type": "dashboard",       "label": "Dashboard",         "category": "Output",     "color": "#9333EA"},
    {"type": "data_store",      "label": "Data Store",        "category": "Storage",    "color": "#475569"},
]


@app.get("/api/node-types")
async def get_node_types():
    """Return available node types for the pipeline builder."""
    return {"node_types": NODE_CATALOG}


# ─────────────────────────────────────────
#  Upload endpoint (for design HTML updates)
# ─────────────────────────────────────────

@app.post("/api/upload-design")
async def upload_design(file: UploadFile = File(...)):
    """
    Upload a new Claude Design HTML file.
    The system will parse it and update the frontend assets.
    """
    if not file.filename.endswith(".html"):
        raise HTTPException(status_code=400, detail="Only HTML files are accepted")

    # Save the uploaded file
    upload_path = UPLOADS_DIR / f"design_{int(datetime.now().timestamp())}.html"
    with open(upload_path, "wb") as f:
        shutil.copyfileobj(file.file, f)

    # Run the update script
    import subprocess
    result = subprocess.run(
        ["python3", str(BASE_DIR / "scripts" / "update_from_design.py"), str(upload_path)],
        capture_output=True, text=True, cwd=str(BASE_DIR)
    )

    if result.returncode != 0:
        return JSONResponse(
            status_code=500,
            content={"success": False, "error": result.stderr, "stdout": result.stdout}
        )

    return {
        "success": True,
        "message": "Design updated successfully",
        "file": str(upload_path.name),
        "output": result.stdout
    }


# ─────────────────────────────────────────
#  Health check
# ─────────────────────────────────────────

@app.get("/api/health")
async def health():
    return {
        "status": "ok",
        "version": "1.0.0",
        "pipelines_count": len(list(PIPELINES_DIR.glob("*.json")))
    }


if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)
