import json
import re
from collections import Counter
from datetime import datetime
from typing import Any, Dict, List, Optional

import pymongo

from config.settings import DATABASE_NAME, MONGODB_URI

ARTICLE_URL_REGEX = r"/\d{3,}.*\.html$"
TOPIC_WORD_REGEX = re.compile(r"[^\W\d_]{4,}", flags=re.UNICODE)
TOPIC_STOPWORDS = {
    "avec", "dans", "pour", "mais", "sans", "plus", "moins", "tres", "trop", "toute", "toutes",
    "tout", "tous", "chez", "leurs", "aussi", "sont", "etre", "etait", "etais", "avait", "avoir",
    "comme", "dont", "afin", "ainsi", "etre", "plus", "moins", "cette", "cet", "ces", "des",
    "une", "un", "aux", "par", "sur", "ses", "son", "sa", "leurs", "dans", "pour", "sans",
    "entre", "apres", "avant", "contre", "depuis", "lors", "chez", "sous", "vers", "tout",
    "tous", "toute", "toutes", "alors", "encore", "autour", "ainsi", "dune", "dun", "pourquoi",
    "this", "that", "from", "with", "your", "their", "there", "about", "into", "over", "under",
    "have", "will", "could", "would", "should", "when", "where", "which", "while", "after",
    "before", "about", "against", "between", "because", "through", "since", "until", "within",
    "these", "those", "only", "also", "been", "were", "being", "than", "then", "just", "most",
    "very", "into", "from", "onto", "over", "that", "this", "what", "your", "their", "they",
    "them", "some", "more", "less", "news", "site", "page", "html", "http", "https", "hespress",
    "maroc", "morocco", "france",
}


def _safe_iso(value: Any) -> Optional[str]:
    if isinstance(value, datetime):
        return value.isoformat()
    return None


def _clean_keywords(value: Any) -> List[str]:
    if not isinstance(value, list):
        return []
    return [str(item).strip() for item in value if isinstance(item, str) and item.strip()]


def _tokenize(text: str) -> List[str]:
    words = TOPIC_WORD_REGEX.findall(text.lower())
    return [word for word in words if word not in TOPIC_STOPWORDS]


def _extract_topics(docs: List[Dict[str, Any]]) -> Counter:
    unigrams: Counter = Counter()
    bigrams: Counter = Counter()

    for doc in docs:
        text = f"{doc.get('title', '')} {doc.get('description', '')}"
        tokens = _tokenize(text)
        for word in tokens:
            unigrams[word] += 1
        for first, second in zip(tokens, tokens[1:]):
            bigrams[f"{first} {second}"] += 1

    topics: Counter = Counter()
    for phrase, count in bigrams.items():
        if count >= 2:
            topics[phrase] += count

    if len(topics) < 12:
        for word, count in unigrams.most_common(40):
            topics[word] += count

    return topics


def _metric(key: str, label: str, value: Any, unit: str = "", fmt: str = "number") -> Dict[str, Any]:
    return {"key": key, "label": label, "value": value, "unit": unit, "format": fmt}


def _extract_json_payload(text: str) -> Optional[Dict[str, Any]]:
    if not text:
        return None
    cleaned = text.strip()
    if "```" in cleaned:
        cleaned = re.sub(r"```[a-zA-Z]*", "", cleaned)
        cleaned = cleaned.replace("```", "")
    match = re.search(r"\{.*\}", cleaned, re.DOTALL)
    if match:
        cleaned = match.group(0)
    try:
        return json.loads(cleaned)
    except Exception:
        return None


class ReportingService:
    """Read-only analytics helper to summarize crawl sessions and invoke LLM reporting."""

    def __init__(self) -> None:
        self._init_error: Optional[str] = None
        self.client: Optional[pymongo.MongoClient] = None
        self.collection: Optional[pymongo.collection.Collection] = None

        try:
            self.client = pymongo.MongoClient(MONGODB_URI, serverSelectionTimeoutMS=4000)
            self.client.admin.command("ping")
            db = self.client[DATABASE_NAME]
            self.collection = db["crawled_data"]
        except Exception as exc:  # pragma: no cover - defensive guard for runtime env
            self._init_error = str(exc)
            self.client = None
            self.collection = None

    # ----- public API -----------------------------------------------------
    def list_sessions(self, limit: int = 8) -> List[Dict[str, Any]]:
        """Return recent crawl sessions (by source_id) ordered by last seen document."""
        col = self._require_collection()
        docs = col.find(
            {},
            {
                "source_id": 1,
                "timestamp": 1,
                "url": 1,
                "keywords_filter": 1,
                "keywords": 1,
            },
        ).sort("timestamp", -1).limit(limit * 40)

        sessions: Dict[str, Dict[str, Any]] = {}
        for doc in docs:
            session_id = str(doc.get("source_id") or "unspecified")
            timestamp = doc.get("timestamp")

            entry = sessions.setdefault(
                session_id,
                {
                    "session_id": session_id,
                    "count": 0,
                    "last_timestamp": None,
                    "sample_url": "",
                    "top_keywords": Counter(),
                },
            )
            entry["count"] += 1
            if timestamp and (entry["last_timestamp"] is None or timestamp > entry["last_timestamp"]):
                entry["last_timestamp"] = timestamp
            if not entry["sample_url"] and doc.get("url"):
                entry["sample_url"] = doc["url"]
            for kw in _clean_keywords(doc.get("keywords_filter") or doc.get("keywords") or []):
                entry["top_keywords"][kw] += 1

        result: List[Dict[str, Any]] = []
        for entry in sessions.values():
            result.append(
                {
                    "session_id": entry["session_id"],
                    "count": entry["count"],
                    "last_timestamp": _safe_iso(entry["last_timestamp"]),
                    "sample_url": entry["sample_url"],
                    "top_keywords": [
                        {"keyword": kw, "count": count} for kw, count in entry["top_keywords"].most_common(8)
                    ],
                }
            )

        return sorted(result, key=lambda item: item.get("last_timestamp") or "", reverse=True)[:limit]

    def summarize_session(self, session_id: Optional[str] = None, sample_limit: int = 14) -> Dict[str, Any]:
        """Aggregate data for the given session_id (or latest if none) for dashboard use."""
        col = self._require_collection()

        resolved_session_id = session_id or self._latest_session_id()
        if not resolved_session_id:
            raise RuntimeError("No crawl data found to build a report.")

        match_query: Dict[str, Any] = {"source_id": resolved_session_id}
        total_docs = col.count_documents(match_query)
        if total_docs == 0:
            raise RuntimeError(f"No documents found for session '{resolved_session_id}'.")

        news_query = {**match_query, "url": {"$regex": ARTICLE_URL_REGEX}}
        news_docs = col.count_documents(news_query)
        use_query = news_query if news_docs else match_query
        if not news_docs:
            news_docs = total_docs

        # Time window
        first_doc = col.find(use_query, {"timestamp": 1}).sort("timestamp", 1).limit(1)
        last_doc = col.find(use_query, {"timestamp": 1}).sort("timestamp", -1).limit(1)
        start_ts = next(iter(first_doc), {}).get("timestamp")
        end_ts = next(iter(last_doc), {}).get("timestamp")

        # Content type distribution
        content_types = list(
            col.aggregate(
                [
                    {"$match": use_query},
                    {"$group": {"_id": "$content_type", "count": {"$sum": 1}}},
                    {"$sort": {"count": -1}},
                ]
            )
        )

        # Domains
        domains = list(
            col.aggregate(
                [
                    {"$match": use_query},
                    {
                        "$project": {
                            "domain": {
                                "$arrayElemAt": [{"$split": ["$url", "/"]}, 2]
                            }
                        }
                    },
                    {"$group": {"_id": "$domain", "count": {"$sum": 1}}},
                    {"$sort": {"count": -1}},
                    {"$limit": 15},
                ]
            )
        )

        # Time histogram (bucketed by hour)
        time_histogram = list(
            col.aggregate(
                [
                    {"$match": use_query},
                    {
                        "$project": {
                            "bucket": {
                                "$dateToString": {"format": "%Y-%m-%d %H:00", "date": "$timestamp"}
                            }
                        }
                    },
                    {"$group": {"_id": "$bucket", "count": {"$sum": 1}}},
                    {"$sort": {"_id": 1}},
                ]
            )
        )

        topic_docs = list(
            col.find(
                use_query,
                {"title": 1, "description": 1},
            ).limit(400)
        )

        topics_counter = _extract_topics(topic_docs)
        top_topics = [{"topic": topic, "count": count} for topic, count in topics_counter.most_common(20)]

        length_stats = next(
            iter(
                col.aggregate(
                    [
                        {"$match": use_query},
                        {
                            "$project": {
                                "title_len": {"$strLenCP": {"$ifNull": ["$title", ""]}},
                                "desc_len": {"$strLenCP": {"$ifNull": ["$description", ""]}},
                                "content_len": {"$strLenCP": {"$ifNull": ["$content", ""]}},
                            }
                        },
                        {
                            "$group": {
                                "_id": None,
                                "avg_desc_len": {"$avg": "$desc_len"},
                                "avg_content_len": {"$avg": "$content_len"},
                                "missing_title": {"$sum": {"$cond": [{"$lte": ["$title_len", 5]}, 1, 0]}},
                                "missing_desc": {"$sum": {"$cond": [{"$lte": ["$desc_len", 30]}, 1, 0]}},
                                "thin_content": {"$sum": {"$cond": [{"$lte": ["$content_len", 500]}, 1, 0]}},
                            }
                        },
                    ]
                )
            ),
            {},
        )

        unique_titles = next(
            iter(
                col.aggregate(
                    [
                        {"$match": use_query},
                        {"$match": {"title": {"$ne": None}}},
                        {"$group": {"_id": "$title"}},
                        {"$count": "count"},
                    ]
                )
            ),
            {},
        ).get("count", 0)

        # Latest documents for display + LLM context
        latest_items_cursor = col.find(
            use_query,
            {
                "title": 1,
                "url": 1,
                "description": 1,
                "content_type": 1,
                "keywords": 1,
                "keywords_filter": 1,
                "timestamp": 1,
            },
        ).sort("timestamp", -1).limit(sample_limit)

        latest_items = []
        for doc in latest_items_cursor:
            latest_items.append(
                {
                    "title": doc.get("title") or "(untitled)",
                    "url": doc.get("url"),
                    "description": (doc.get("description") or "")[:300],
                    "content_type": doc.get("content_type"),
                    "keywords": _clean_keywords(doc.get("keywords") or doc.get("keywords_filter") or []),
                    "timestamp": _safe_iso(doc.get("timestamp")),
                }
            )

        total_items = news_docs
        article_ratio = total_items / total_docs if total_docs else 0
        domains_count = len(domains)
        top_domain_share = (domains[0]["count"] / total_items) if domains and total_items else 0
        domain_concentration_hhi = (
            sum((item["count"] / total_items) ** 2 for item in domains) if total_items else 0
        )

        time_window_hours = 0.0
        if start_ts and end_ts:
            time_window_hours = max((end_ts - start_ts).total_seconds() / 3600, 0.0)
        items_per_hour = total_items / time_window_hours if time_window_hours else float(total_items)

        missing_title = length_stats.get("missing_title", 0) or 0
        missing_desc = length_stats.get("missing_desc", 0) or 0
        thin_content = length_stats.get("thin_content", 0) or 0
        avg_desc_len = float(length_stats.get("avg_desc_len") or 0)
        avg_content_len = float(length_stats.get("avg_content_len") or 0)
        missing_title_rate = missing_title / total_items if total_items else 0
        missing_description_rate = missing_desc / total_items if total_items else 0
        thin_content_rate = thin_content / total_items if total_items else 0
        unique_title_rate = unique_titles / total_items if total_items else 0

        topics_count = len(topics_counter)
        total_topic_mentions = sum(topics_counter.values()) or 1
        topics_per_article = total_topic_mentions / total_items if total_items else 0
        top_topic_share = (top_topics[0]["count"] / total_topic_mentions) if top_topics else 0

        metrics = {
            "total_items": _metric("total_items", "News articles", total_items, "articles", "number"),
            "raw_total_items": _metric("raw_total_items", "All items in session", total_docs, "items", "number"),
            "article_ratio": _metric("article_ratio", "Article ratio", article_ratio, "%", "percent"),
            "unique_titles": _metric("unique_titles", "Unique titles", unique_titles, "titles", "number"),
            "unique_title_rate": _metric("unique_title_rate", "Unique title rate", unique_title_rate, "%", "percent"),
            "domains_count": _metric("domains_count", "Domains", domains_count, "domains", "number"),
            "top_domain_share": _metric("top_domain_share", "Top domain share", top_domain_share, "%", "percent"),
            "domain_concentration_hhi": _metric(
                "domain_concentration_hhi", "Domain concentration (HHI)", domain_concentration_hhi, "", "float3"
            ),
            "time_window_hours": _metric("time_window_hours", "Time window", time_window_hours, "hours", "hours"),
            "items_per_hour": _metric("items_per_hour", "Items per hour", items_per_hour, "items/hr", "float1"),
            "missing_title_rate": _metric(
                "missing_title_rate", "Missing/short titles", missing_title_rate, "%", "percent"
            ),
            "missing_description_rate": _metric(
                "missing_description_rate", "Missing/short descriptions", missing_description_rate, "%", "percent"
            ),
            "avg_description_length": _metric(
                "avg_description_length", "Avg description length", avg_desc_len, "chars", "number"
            ),
            "avg_content_length": _metric(
                "avg_content_length", "Avg content length", avg_content_len, "chars", "number"
            ),
            "thin_content_rate": _metric(
                "thin_content_rate", "Thin content rate (<500 chars)", thin_content_rate, "%", "percent"
            ),
            "topics_count": _metric("topics_count", "Unique topics", topics_count, "topics", "number"),
            "topics_per_article": _metric(
                "topics_per_article", "Topic mentions per article", topics_per_article, "ratio", "float2"
            ),
            "top_topic_share": _metric("top_topic_share", "Top topic share", top_topic_share, "%", "percent"),
        }

        gqm = {
            "goals": [
                {
                    "id": "coverage",
                    "title": "Coverage",
                    "goal": "Measure how much news was captured and over what window.",
                    "questions": [
                        {
                            "id": "coverage_volume",
                            "text": "How much news content was captured in this session?",
                            "metrics": [
                                metrics["total_items"],
                                metrics["unique_titles"],
                                metrics["domains_count"],
                                metrics["article_ratio"],
                            ],
                        },
                        {
                            "id": "coverage_freshness",
                            "text": "How recent and continuous is the coverage?",
                            "metrics": [
                                metrics["time_window_hours"],
                                metrics["items_per_hour"],
                            ],
                        },
                    ],
                },
                {
                    "id": "quality",
                    "title": "Content Quality",
                    "goal": "Assess completeness of headlines and descriptions.",
                    "questions": [
                        {
                            "id": "quality_metadata",
                            "text": "Are titles and descriptions complete?",
                            "metrics": [
                                metrics["missing_title_rate"],
                                metrics["missing_description_rate"],
                                metrics["avg_description_length"],
                            ],
                        },
                        {
                            "id": "quality_depth",
                            "text": "Is the article text substantial?",
                            "metrics": [
                                metrics["avg_content_length"],
                                metrics["thin_content_rate"],
                            ],
                        },
                    ],
                },
                {
                    "id": "diversity",
                    "title": "Diversity & Bias",
                    "goal": "Detect source and topic concentration in the coverage.",
                    "questions": [
                        {
                            "id": "diversity_domains",
                            "text": "Is coverage dominated by one domain?",
                            "metrics": [
                                metrics["top_domain_share"],
                                metrics["domain_concentration_hhi"],
                            ],
                        },
                        {
                            "id": "diversity_topics",
                            "text": "Are headline topics varied or repetitive?",
                            "metrics": [
                                metrics["topics_count"],
                                metrics["topics_per_article"],
                                metrics["top_topic_share"],
                                metrics["unique_title_rate"],
                            ],
                        },
                    ],
                },
            ]
        }

        return {
            "session_id": resolved_session_id,
            "total_items": total_items,
            "raw_total_items": total_docs,
            "news_scope": {
                "filtered": bool(news_docs != total_docs),
                "news_items": news_docs,
                "total_items": total_docs,
            },
            "time_window": {"start": _safe_iso(start_ts), "end": _safe_iso(end_ts)},
            "content_types": [{"type": item["_id"] or "unknown", "count": item["count"]} for item in content_types],
            "top_domains": [{"domain": item["_id"] or "unknown", "count": item["count"]} for item in domains],
            "time_histogram": [{"bucket": item["_id"], "count": item["count"]} for item in time_histogram],
            "top_topics": top_topics,
            "gqm": gqm,
            "latest_items": latest_items,
        }

    def generate_llm_report(self, session_id: Optional[str], instructions: str = "") -> Dict[str, Any]:
        """Call the LLM to craft a concise report over the requested session."""
        summary = self.summarize_session(session_id)
        payload = {
            "session_id": summary["session_id"],
            "time_window": summary["time_window"],
            "content_types": summary["content_types"],
            "top_domains": summary["top_domains"],
            "top_topics": summary["top_topics"],
            "examples": summary["latest_items"][:8],
            "total_items": summary["total_items"],
            "news_scope": summary.get("news_scope"),
            "gqm": summary.get("gqm"),
        }

        system_prompt = (
            "You are a lead data analyst creating crisp executive crawl reports. "
            "Be concrete, highlight risks, patterns, and actionable next steps. "
            "Stay under 250 words. Output plain text with short section headers and bullets."
        )

        user_prompt = (
            "Crawl dataset summary:\n"
            f"{json.dumps(payload, default=str, ensure_ascii=False)}\n\n"
            "Produce:\n"
            "- A GQM summary: answer each goal using its questions and metrics\n"
            "- Highlight anomalies or risks using the metrics\n"
            "- 3-5 next actions tied to the metrics\n"
            "- Chart suggestions (title + axis variables) for the dashboard\n"
        )

        if instructions:
            user_prompt += f"\nTeam instructions: {instructions.strip()}\n"

        report_text = self._call_llm(user_prompt, system_prompt)

        return {
            "session_id": summary["session_id"],
            "report": report_text,
            "time_window": summary["time_window"],
            "total_items": summary["total_items"],
        }

    def summarize_page(self, session_id: Optional[str], url: str) -> Dict[str, Any]:
        """Analyze a single page and extract chartable data when possible."""
        if not url:
            raise RuntimeError("URL is required for page analysis.")

        col = self._require_collection()
        query: Dict[str, Any] = {"url": url}
        if session_id:
            query["source_id"] = session_id

        doc = col.find_one(
            query,
            {
                "title": 1,
                "description": 1,
                "content": 1,
                "timestamp": 1,
                "content_type": 1,
            },
        )
        if not doc and session_id:
            doc = col.find_one(
                {"url": url},
                {
                    "title": 1,
                    "description": 1,
                    "content": 1,
                    "timestamp": 1,
                    "content_type": 1,
                },
            )

        if not doc:
            raise RuntimeError("Page not found in crawl data.")

        title = (doc.get("title") or "").strip()
        description = (doc.get("description") or "").strip()
        content = (doc.get("content") or "").strip()
        clipped_content = content[:6000]

        system_prompt = (
            "You extract quantitative facts from a single news article and build charts. "
            "Only use numbers stated in the text. Do not guess. "
            "Return JSON only, no markdown."
        )

        user_prompt = (
            "Article text:\n"
            f"TITLE: {title}\n"
            f"DESCRIPTION: {description}\n"
            f"CONTENT: {clipped_content}\n\n"
            "Return JSON with this schema:\n"
            "{\n"
            "  \"summary\": \"2-3 sentence summary of the most notable facts\",\n"
            "  \"insights\": [\n"
            "    {\"title\": \"\", \"detail\": \"\", \"evidence\": \"\"}\n"
            "  ],\n"
            "  \"charts\": [\n"
            "    {\n"
            "      \"title\": \"\",\n"
            "      \"type\": \"line|bar|pie|area\",\n"
            "      \"x_label\": \"\",\n"
            "      \"y_label\": \"\",\n"
            "      \"series\": [\n"
            "        {\"name\": \"\", \"data\": [{\"x\": \"\", \"y\": 0.0}]}\n"
            "      ],\n"
            "      \"notes\": \"\"\n"
            "    }\n"
            "  ]\n"
            "}\n"
            "Rules:\n"
            "- Use at most 3 charts and at most 12 points per series.\n"
            "- If no chartable numbers exist, return charts: [].\n"
            "- Use numbers as floats with dot, and x values as strings.\n"
            "- Evidence should quote a short snippet from the text.\n"
        )

        try:
            raw = self._call_llm_raw(user_prompt, system_prompt)
        except Exception as exc:
            return {
                "url": url,
                "title": title,
                "timestamp": _safe_iso(doc.get("timestamp")),
                "content_type": doc.get("content_type"),
                "summary": f"LLM error: {exc}",
                "insights": [],
                "charts": [],
            }

        parsed = _extract_json_payload(raw) if raw else None

        summary = ""
        insights = []
        charts = []

        if parsed:
            summary = str(parsed.get("summary") or "").strip()
            insights = parsed.get("insights") or []
            charts = parsed.get("charts") or []

        if not summary:
            summary = "No chartable data was detected in this article."

        if not isinstance(insights, list):
            insights = []
        normalized_insights = []
        for insight in insights[:5]:
            if isinstance(insight, str):
                normalized_insights.append({"title": "Insight", "detail": insight, "evidence": ""})
                continue
            if isinstance(insight, dict):
                normalized_insights.append(
                    {
                        "title": str(insight.get("title") or "Insight"),
                        "detail": str(insight.get("detail") or insight.get("description") or ""),
                        "evidence": str(insight.get("evidence") or ""),
                    }
                )

        def coerce_float(value: Any) -> Optional[float]:
            try:
                return float(value)
            except Exception:
                return None

        allowed_types = {"line", "bar", "pie", "area"}
        cleaned_charts = []
        if isinstance(charts, list):
            for chart in charts:
                if not isinstance(chart, dict):
                    continue
                chart_type = str(chart.get("type") or "").lower()
                if chart_type not in allowed_types:
                    continue
                series = chart.get("series") or []
                if not isinstance(series, list):
                    series = []
                cleaned_series = []
                for serie in series:
                    if not isinstance(serie, dict):
                        continue
                    data = serie.get("data") or []
                    if not isinstance(data, list):
                        continue
                    points = []
                    for point in data[:12]:
                        if not isinstance(point, dict):
                            continue
                        x_value = point.get("x")
                        y_value = coerce_float(point.get("y"))
                        if y_value is None:
                            continue
                        points.append({"x": str(x_value), "y": y_value})
                    if points:
                        cleaned_series.append({"name": str(serie.get("name") or "Series"), "data": points})
                if not cleaned_series:
                    continue
                cleaned_charts.append(
                    {
                        "title": str(chart.get("title") or "Chart"),
                        "type": chart_type,
                        "x_label": str(chart.get("x_label") or ""),
                        "y_label": str(chart.get("y_label") or ""),
                        "series": cleaned_series,
                        "notes": str(chart.get("notes") or ""),
                    }
                )
                if len(cleaned_charts) >= 3:
                    break

        return {
            "url": url,
            "title": title,
            "timestamp": _safe_iso(doc.get("timestamp")),
            "content_type": doc.get("content_type"),
            "summary": summary,
            "insights": normalized_insights,
            "charts": cleaned_charts,
        }

    # ----- helpers -------------------------------------------------------
    def _require_collection(self) -> pymongo.collection.Collection:
        if self.collection is None:
            raise RuntimeError(self._init_error or "Reporting unavailable: MongoDB connection failed.")
        return self.collection

    def _latest_session_id(self) -> Optional[str]:
        col = self._require_collection()
        latest = col.find({"source_id": {"$exists": True}}, {"source_id": 1}).sort("timestamp", -1).limit(1)
        doc = next(iter(latest), None)
        return str(doc["source_id"]) if doc and doc.get("source_id") else None

    def _call_llm(self, prompt: str, system_prompt: str) -> str:
        try:
            from llm.client import call_groq, LAST_ERROR
        except Exception as exc:  # pragma: no cover - runtime guard
            return f"LLM unavailable: {exc}"

        try:
            response = call_groq(prompt, system=system_prompt)
            if response:
                return response
            if LAST_ERROR:
                return LAST_ERROR
            return "LLM returned no content."
        except Exception as exc:  # pragma: no cover - runtime guard
            return f"LLM error: {exc}"

    def _call_llm_raw(self, prompt: str, system_prompt: str) -> str:
        try:
            from llm.client import call_groq, LAST_ERROR
        except Exception as exc:  # pragma: no cover - runtime guard
            raise RuntimeError(f"LLM unavailable: {exc}") from exc

        response = call_groq(prompt, system=system_prompt)
        if response:
            return response
        if LAST_ERROR:
            raise RuntimeError(LAST_ERROR)
        raise RuntimeError("LLM returned no content.")
