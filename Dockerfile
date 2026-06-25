FROM python:3.11-slim

WORKDIR /app

# Install dependencies
COPY backend/requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

# Copy backend code
COPY backend/ ./backend/

# Copy frontend (served as static files)
COPY frontend/ ./frontend/

# Copy scripts (for the upload-design endpoint)
COPY scripts/ ./scripts/

# Data directory for pipelines
RUN mkdir -p data/pipelines uploads/backups

EXPOSE 8000

CMD ["uvicorn", "backend.main:app", "--host", "0.0.0.0", "--port", "8000"]
