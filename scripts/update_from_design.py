#!/usr/bin/env python3
"""
update_from_design.py
─────────────────────
Extracts JS/CSS/fonts from a Claude Design standalone HTML bundle
and updates the frontend directory automatically.

Usage:
    python3 scripts/update_from_design.py <path-to-html>
"""
import sys
import re
import json
import gzip
import base64
import shutil
from pathlib import Path
from datetime import datetime

BASE_DIR     = Path(__file__).parent.parent
FRONTEND_DIR = BASE_DIR / "frontend"
BACKUP_DIR   = BASE_DIR / "uploads" / "backups"

for d in [BACKUP_DIR, FRONTEND_DIR,
          FRONTEND_DIR / "js",
          FRONTEND_DIR / "css",
          FRONTEND_DIR / "fonts"]:
    d.mkdir(parents=True, exist_ok=True)


def log(msg: str):
    print(f"[update_from_design] {msg}")


def backup_frontend():
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup_path = BACKUP_DIR / f"frontend_backup_{ts}"
    if FRONTEND_DIR.exists():
        shutil.copytree(FRONTEND_DIR, backup_path)
        log(f"Backup created: {backup_path.name}")
    return backup_path


def decompress_asset(entry: dict) -> bytes:
    raw = base64.b64decode(entry["data"])
    if entry.get("compressed"):
        raw = gzip.decompress(raw)
    return raw


def extract_bundle(html_path: Path):
    content = html_path.read_text(encoding="utf-8", errors="replace")
    manifest_match = re.search(
        r'<script[^>]+type="__bundler/manifest"[^>]*>(.*?)</script>',
        content, re.DOTALL
    )
    template_match = re.search(
        r'<script[^>]+type="__bundler/template"[^>]*>(.*?)</script>',
        content, re.DOTALL
    )
    if manifest_match and template_match:
        manifest = json.loads(manifest_match.group(1).strip())
        template = json.loads(template_match.group(1).strip())
        log(f"Found bundler manifest with {len(manifest)} assets")
        return manifest, template
    log("No bundler manifest — treating as plain HTML")
    return None, content


def update_from_bundle(manifest: dict, template: str):
    js_parts  = []
    css_parts = []
    font_map  = {}   # uuid → static path

    for uuid, entry in manifest.items():
        try:
            data = decompress_asset(entry)
            mime = entry.get("mime", "")

            if "javascript" in mime or mime.endswith("/js"):
                js_parts.append(data.decode("utf-8", errors="replace"))
                log(f"  JS  {uuid[:8]}  {len(data):>8} bytes")

            elif "css" in mime:
                css_parts.append(data.decode("utf-8", errors="replace"))
                log(f"  CSS {uuid[:8]}  {len(data):>8} bytes")

            elif "font" in mime:
                ext  = mime.split("/")[-1]
                fname = f"{uuid[:8]}.{ext}"
                (FRONTEND_DIR / "fonts" / fname).write_bytes(data)
                font_map[uuid] = f"/static/fonts/{fname}"
                log(f"  Font {uuid[:8]}  {len(data):>8} bytes → fonts/{fname}")

        except Exception as e:
            log(f"  Warning: {uuid[:8]}: {e}")

    # Write CSS
    if css_parts:
        p = FRONTEND_DIR / "css" / "app.css"
        p.write_text("\n\n".join(css_parts), encoding="utf-8")
        log(f"Wrote css/app.css  ({p.stat().st_size} bytes)")

    # Write JS (patch font UUID refs → static paths)
    if js_parts:
        combined = "\n\n".join(js_parts)
        for uuid, static_path in font_map.items():
            combined = combined.replace(uuid, static_path)
        p = FRONTEND_DIR / "js" / "app.js"
        p.write_text(combined, encoding="utf-8")
        log(f"Wrote js/app.js  ({p.stat().st_size} bytes)")

    # Rebuild index.html
    css_link  = '<link rel="stylesheet" href="/static/css/app.css">' if css_parts  else ""
    js_script = '<script src="/static/js/app.js"></script>'           if js_parts  else ""

    index_html = f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>DoTwin — Pipeline Builder</title>
  {css_link}
  <style>
    #api-status {{
      position: fixed; bottom: 12px; left: 12px;
      font: 11px/1.4 ui-monospace, monospace; color: #666;
      background: rgba(255,255,255,0.9); padding: 4px 10px;
      border-radius: 6px; box-shadow: 0 1px 4px rgba(0,0,0,0.1);
      z-index: 99999; pointer-events: none;
    }}
    #api-status.connected {{ color: #16a34a; }}
    #api-status.error     {{ color: #dc2626; }}
  </style>
</head>
<body>
  <div id="root"></div>
  <div id="api-status">⟳ connecting…</div>

  <!-- DoTwin API Bridge -->
  <script>
  (function() {{
    const BASE = '';
    window.DoTwinAPI = {{
      async listPipelines()        {{ return (await fetch(BASE+'/api/pipelines')).json(); }},
      async getPipeline(id)        {{ const r=await fetch(BASE+`/api/pipelines/${{id}}`); if(!r.ok) throw new Error('Not found'); return r.json(); }},
      async savePipeline(p)        {{
        const id=p.id, method=id?'PUT':'POST', url=id?BASE+`/api/pipelines/${{id}}`:BASE+'/api/pipelines';
        return (await fetch(url,{{method,headers:{{'Content-Type':'application/json'}},body:JSON.stringify(p)}})).json();
      }},
      async deletePipeline(id)     {{ return (await fetch(BASE+`/api/pipelines/${{id}}`,{{method:'DELETE'}})).json(); }},
      async getNodeTypes()         {{ return (await fetch(BASE+'/api/node-types')).json(); }},
      async health()               {{ return (await fetch(BASE+'/api/health')).json(); }}
    }};
    const el = document.getElementById('api-status');
    window.DoTwinAPI.health()
      .then(d => {{ el.textContent=`✓ API v${{d.version}} · ${{d.pipelines_count}} pipelines`; el.className='connected'; }})
      .catch(()  => {{ el.textContent='✗ API offline (standalone mode)'; el.className='error'; }});
  }})();
  </script>

  {js_script}
</body>
</html>
"""
    (FRONTEND_DIR / "index.html").write_text(index_html, encoding="utf-8")
    log("Wrote frontend/index.html")


def update_from_plain_html(html_content: str):
    (FRONTEND_DIR / "index.html").write_text(html_content, encoding="utf-8")
    log("Wrote plain HTML to frontend/index.html")


def main():
    if len(sys.argv) < 2:
        print("Usage: python3 update_from_design.py <html-file>")
        sys.exit(1)

    html_path = Path(sys.argv[1])
    if not html_path.exists():
        print(f"File not found: {html_path}")
        sys.exit(1)

    log(f"Processing: {html_path.name}")
    backup_frontend()
    manifest, template = extract_bundle(html_path)

    if manifest is None:
        update_from_plain_html(template)
    else:
        update_from_bundle(manifest, template)

    log("✓ Frontend updated successfully")


if __name__ == "__main__":
    main()
