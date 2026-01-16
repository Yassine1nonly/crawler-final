import json
import os
import sys

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if BASE_DIR not in sys.path:
    sys.path.insert(0, BASE_DIR)

from flask import Flask, Response, jsonify, request

from server.manager import CrawlerManager
from server.reporting import ReportingService

FRONTEND_DIR = os.path.join(BASE_DIR, "frontend")

app = Flask(__name__, static_folder=FRONTEND_DIR, static_url_path="")
manager = CrawlerManager()
reporting = ReportingService()


@app.route("/")
def index():
    return app.send_static_file("index.html")


@app.route("/api/jobs", methods=["GET"])
def jobs():
    return jsonify({"jobs": manager.list_stats()})


@app.route("/api/crawl/start", methods=["POST"])
def start_crawl():
    payload = request.get_json(silent=True) or {}
    url = (payload.get("url") or "").strip()
    if not url:
        return jsonify({"error": "URL is required"}), 400

    if not url.startswith(("http://", "https://")):
        url = "https://" + url

    max_pages = int(payload.get("max_pages") or 5)
    content_types = payload.get("content_types") or ["html"]
    if not isinstance(content_types, list):
        content_types = ["html"]

    keywords = payload.get("keywords") or []
    if isinstance(keywords, str):
        keywords = [k.strip() for k in keywords.replace(",", " ").split() if k.strip()]
    elif not isinstance(keywords, list):
        keywords = []

    job_id = manager.start(url, max_pages=max_pages, content_types=content_types, keywords=keywords)
    return jsonify({"job_id": job_id})


@app.route("/api/crawl/pause", methods=["POST"])
def pause_crawl():
    payload = request.get_json(silent=True) or {}
    job_id = payload.get("job_id")
    if not job_id:
        return jsonify({"error": "job_id is required"}), 400
    if not manager.pause(job_id):
        return jsonify({"error": "job not found"}), 404
    return jsonify({"ok": True})


@app.route("/api/crawl/resume", methods=["POST"])
def resume_crawl():
    payload = request.get_json(silent=True) or {}
    job_id = payload.get("job_id")
    if not job_id:
        return jsonify({"error": "job_id is required"}), 400
    if not manager.resume(job_id):
        return jsonify({"error": "job not found"}), 404
    return jsonify({"ok": True})


@app.route("/api/crawl/stop", methods=["POST"])
def stop_crawl():
    payload = request.get_json(silent=True) or {}
    job_id = payload.get("job_id")
    if not job_id:
        return jsonify({"error": "job_id is required"}), 400
    if not manager.stop(job_id):
        return jsonify({"error": "job not found"}), 404
    return jsonify({"ok": True})


@app.route("/api/crawl/delete", methods=["POST"])
def delete_crawl():
    payload = request.get_json(silent=True) or {}
    job_id = payload.get("job_id")
    if not job_id:
        return jsonify({"error": "job_id is required"}), 400
    if not manager.delete(job_id):
        return jsonify({"error": "job not found"}), 404
    return jsonify({"ok": True})


@app.route("/api/stream")
def stream():
    def event_stream():
        q = manager.subscribe()
        try:
            snapshot = {"type": "snapshot", "jobs": manager.list_stats()}
            yield f"data: {json.dumps(snapshot)}\n\n"
            while True:
                data = q.get()
                yield f"data: {data}\n\n"
        finally:
            manager.unsubscribe(q)

    headers = {
        "Cache-Control": "no-cache",
        "X-Accel-Buffering": "no",
    }
    return Response(event_stream(), headers=headers, mimetype="text/event-stream")


@app.route("/api/reports/sessions", methods=["GET"])
def report_sessions():
    try:
        return jsonify({"sessions": reporting.list_sessions()})
    except Exception as exc:
        return jsonify({"error": str(exc)}), 500


@app.route("/api/reports/session", methods=["GET"])
def report_session_detail():
    session_id = request.args.get("session_id")
    try:
        summary = reporting.summarize_session(session_id)
        return jsonify(summary)
    except Exception as exc:
        return jsonify({"error": str(exc)}), 500


@app.route("/api/reports/run", methods=["POST"])
def run_report():
    payload = request.get_json(silent=True) or {}
    session_id = payload.get("session_id")
    instructions = payload.get("instructions", "")
    try:
        report = reporting.generate_llm_report(session_id, instructions=instructions)
        return jsonify(report)
    except Exception as exc:
        return jsonify({"error": str(exc)}), 500


@app.route("/api/reports/page", methods=["POST"])
def report_page():
    payload = request.get_json(silent=True) or {}
    session_id = payload.get("session_id")
    url = payload.get("url")
    if not url:
        return jsonify({"error": "url is required"}), 400
    try:
        summary = reporting.summarize_page(session_id, url)
        return jsonify(summary)
    except Exception as exc:
        return jsonify({"error": str(exc)}), 500


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8000, debug=True, threaded=True)
