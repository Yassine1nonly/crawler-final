import json
import threading
import time
import uuid
from dataclasses import dataclass, asdict
from queue import Queue
from typing import Dict, List, Optional

from crawler.web_crawler import WebCrawler


class CrawlerControl:
    def __init__(self) -> None:
        self.pause_event = threading.Event()
        self.pause_event.set()
        self.stop_event = threading.Event()


@dataclass
class CrawlerStats:
    job_id: str
    url: str
    max_pages: int
    status: str
    start_time: Optional[float]
    last_update: Optional[float]
    pages_attempted: int
    pages_success: int
    errors: int
    pages_per_sec: float
    last_url: str
    last_error: str
    queue_size: int

    def to_dict(self) -> Dict:
        return asdict(self)


class CrawlerManager:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._jobs: Dict[str, Dict] = {}
        self._subscribers: List[Queue] = []

    def start(self, url: str, max_pages: int, content_types: List[str], keywords: List[str]) -> str:
        job_id = uuid.uuid4().hex[:8]
        control = CrawlerControl()
        stats = CrawlerStats(
            job_id=job_id,
            url=url,
            max_pages=max_pages,
            status="running",
            start_time=time.time(),
            last_update=time.time(),
            pages_attempted=0,
            pages_success=0,
            errors=0,
            pages_per_sec=0.0,
            last_url="",
            last_error="",
            queue_size=0,
        )

        thread = threading.Thread(
            target=self._run_job,
            args=(job_id, url, max_pages, content_types, keywords, control),
            daemon=True,
        )

        with self._lock:
            self._jobs[job_id] = {
                "thread": thread,
                "control": control,
                "stats": stats,
                "content_types": content_types,
                "keywords": keywords,
            }

        self._publish({"type": "job_started", "job": stats.to_dict()})
        thread.start()
        return job_id

    def pause(self, job_id: str) -> bool:
        job = self._jobs.get(job_id)
        if not job:
            return False
        job["control"].pause_event.clear()
        self._set_status(job_id, "paused")
        return True

    def resume(self, job_id: str) -> bool:
        job = self._jobs.get(job_id)
        if not job:
            return False
        job["control"].pause_event.set()
        self._set_status(job_id, "running")
        return True

    def stop(self, job_id: str) -> bool:
        job = self._jobs.get(job_id)
        if not job:
            return False
        job["control"].stop_event.set()
        job["control"].pause_event.set()
        self._set_status(job_id, "stopping")
        return True

    def list_stats(self) -> List[Dict]:
        with self._lock:
            return [job["stats"].to_dict() for job in self._jobs.values()]

    def subscribe(self) -> Queue:
        q: Queue = Queue()
        with self._lock:
            self._subscribers.append(q)
        return q

    def unsubscribe(self, q: Queue) -> None:
        with self._lock:
            if q in self._subscribers:
                self._subscribers.remove(q)

    def _publish(self, payload: Dict) -> None:
        data = json.dumps(payload)
        with self._lock:
            subscribers = list(self._subscribers)
        for q in subscribers:
            q.put(data)

    def _set_status(self, job_id: str, status: str) -> None:
        with self._lock:
            job = self._jobs.get(job_id)
            if not job:
                return
            job["stats"].status = status
            job["stats"].last_update = time.time()
            stats = job["stats"].to_dict()
        self._publish({"type": "stats", "jobs": [stats]})

    def _run_job(self, job_id: str, url: str, max_pages: int, content_types: List[str], keywords: List[str], control: CrawlerControl) -> None:
        crawler = WebCrawler()

        def stats_cb(event: str, payload: Dict) -> None:
            self._handle_event(job_id, event, payload)

        try:
            results = crawler.crawl_url(
                url,
                content_types=content_types,
                max_hits=max_pages,
                keywords=keywords,
                control=control,
                stats_cb=stats_cb,
            )
            stored = 0
            for item in results:
                item["source_id"] = job_id
                item["keywords_filter"] = keywords
                try:
                    crawler.data_collection.insert_one(item)
                    stored += 1
                except Exception:
                    pass
        except Exception as exc:
            self._handle_event(job_id, "error", {"url": url, "error": str(exc)})
            self._set_status(job_id, "error")
        finally:
            try:
                crawler.close()
            except Exception:
                pass

    def _handle_event(self, job_id: str, event: str, payload: Dict) -> None:
        with self._lock:
            job = self._jobs.get(job_id)
            if not job:
                return

            stats: CrawlerStats = job["stats"]
            now = time.time()
            stats.last_update = now

            if event == "attempt":
                stats.pages_attempted += 1
                stats.last_url = payload.get("url", stats.last_url)
                stats.queue_size = payload.get("queue", stats.queue_size)
            elif event == "success":
                stats.pages_success += 1
            elif event == "error":
                stats.errors += 1
                stats.last_error = payload.get("error", stats.last_error)
            elif event == "stopped":
                stats.status = "stopped"
            elif event == "done":
                stats.status = "done"

            elapsed = max(now - (stats.start_time or now), 0.001)
            stats.pages_per_sec = stats.pages_success / elapsed

            updated = stats.to_dict()

        self._publish({"type": "stats", "jobs": [updated]})
