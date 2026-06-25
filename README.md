# DoTwin Pipeline Builder

A full-stack Pipeline Builder extracted from Claude Design, backed by FastAPI.

## Project Structure

```
dotwin-pipeline/
├── backend/
│   ├── main.py              # FastAPI app (all API endpoints)
│   └── requirements.txt
├── frontend/
│   ├── index.html           # Main page (updated by script)
│   ├── js/app.js            # Extracted JS bundle
│   ├── css/app.css          # Extracted CSS
│   └── fonts/               # Extracted web fonts
├── scripts/
│   └── update_from_design.py  # Auto-update script
├── data/
│   └── pipelines/           # Saved pipeline JSON files
└── uploads/
    └── backups/             # Frontend backups before each update
```

## Setup

```bash
cd backend
pip install -r requirements.txt
```

## Run

```bash
cd backend
uvicorn main:app --reload --host 0.0.0.0 --port 8000
```

Then open: http://localhost:8000

## API Endpoints

| Method | Path | Description |
|--------|------|-------------|
| GET | `/api/pipelines` | List all pipelines |
| GET | `/api/pipelines/{id}` | Get a pipeline |
| POST | `/api/pipelines` | Create a pipeline |
| PUT | `/api/pipelines/{id}` | Update a pipeline |
| DELETE | `/api/pipelines/{id}` | Delete a pipeline |
| GET | `/api/node-types` | Get available node types |
| POST | `/api/upload-design` | Upload new Claude Design HTML |
| GET | `/api/health` | Health check |

## Workflow: Updating from Claude Design

هر بار که خروجی جدیدی از Claude Design دارید:

### روش ۱ — از طریق API (پیشنهادی)
```bash
curl -X POST http://localhost:8000/api/upload-design \
  -F "file=@DoTwin-PipelineBuilder-standalone.html"
```

### روش ۲ — مستقیم از command line
```bash
python3 scripts/update_from_design.py /path/to/DoTwin-PipelineBuilder-standalone.html
```

هر بار اجرا:
1. یک backup از `frontend/` در `uploads/backups/` می‌سازد
2. JS، CSS و فونت‌ها را از bundle استخراج می‌کند
3. `frontend/index.html` را با API bridge به‌روز می‌کند

## Frontend API Bridge

`window.DoTwinAPI` در browser در دسترس است:

```js
// ذخیره pipeline
await window.DoTwinAPI.savePipeline({ name: "SAG Mill", nodes: [...], edges: [...] });

// لیست pipelines
const { pipelines } = await window.DoTwinAPI.listPipelines();
```
