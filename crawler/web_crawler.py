import requests
from bs4 import BeautifulSoup
import pymongo
from datetime import datetime, timedelta
import schedule
import time
import pdfplumber
import io
from typing import List, Dict
import threading
import logging
from urllib.parse import urljoin, urlparse, parse_qs, urlencode
import random
import hashlib
import json
import re
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
from collections import defaultdict

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class AdvancedAntiBlockingStrategy:
    """Stratégies anti-blocage avancées pour le crawling"""
    
    # User-Agents réalistes et diversifiés
    USER_AGENTS = [
        # Chrome Windows
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36',
        # Chrome Mac
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 13_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36',
        # Firefox
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0',
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:121.0) Gecko/20100101 Firefox/121.0',
        # Safari
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.1 Safari/605.1.15',
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 13_6) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Safari/605.1.15',
        # Edge
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Edge/120.0.0.0',
        # Linux
        'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    ]
    
    # Referers réalistes
    REFERERS = [
        'https://www.google.com/',
        'https://www.google.com/search?q=',
        'https://www.bing.com/',
        'https://duckduckgo.com/',
        'https://www.yahoo.com/',
        '',  # Pas de referer parfois
    ]
    
    # Langues communes
    LANGUAGES = [
        'en-US,en;q=0.9',
        'en-GB,en;q=0.9',
        'fr-FR,fr;q=0.9,en;q=0.8',
        'es-ES,es;q=0.9,en;q=0.8',
        'de-DE,de;q=0.9,en;q=0.8',
    ]
    
    PROXIES = []
    
    def __init__(self):
        self.session_fingerprint = self._generate_fingerprint()
        self.cookies_store = {}
    
    @staticmethod
    def _generate_fingerprint():
        """Génère une empreinte unique pour la session"""
        return hashlib.md5(str(time.time()).encode()).hexdigest()[:16]
    
    @staticmethod
    def get_random_user_agent():
        """Retourne un User-Agent aléatoire"""
        return random.choice(AdvancedAntiBlockingStrategy.USER_AGENTS)
    
    @staticmethod
    def get_random_proxy():
        """Retourne un proxy aléatoire"""
        if AdvancedAntiBlockingStrategy.PROXIES:
            return random.choice(AdvancedAntiBlockingStrategy.PROXIES)
        return None
    
    def get_advanced_headers(self, url=None, referer=None):
        """Génère des headers avancés et réalistes"""
        headers = {
            'User-Agent': self.get_random_user_agent(),
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8',
            'Accept-Language': random.choice(self.LANGUAGES),
            'Accept-Encoding': 'gzip, deflate',
            'DNT': '1',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
            'Sec-Fetch-Dest': 'document',
            'Sec-Fetch-Mode': 'navigate',
            'Sec-Fetch-Site': 'none' if not referer else 'same-origin',
            'Sec-Fetch-User': '?1',
            'Cache-Control': 'max-age=0',
            'sec-ch-ua': '"Not_A Brand";v="8", "Chromium";v="120"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': '"Windows"',
        }
        
        # Ajouter referer de manière intelligente
        if referer:
            headers['Referer'] = referer
        elif url and random.random() > 0.3:  # 70% du temps, ajouter un referer
            ref = random.choice(self.REFERERS)
            if 'search?q=' in ref:
                # Simuler une recherche Google
                domain = urlparse(url).netloc
                headers['Referer'] = f"{ref}{domain.replace('www.', '')}"
            else:
                headers['Referer'] = ref
        
        return headers
    
    @staticmethod
    def calculate_intelligent_delay(base_delay=2, domain=None, is_retry=False):
        """Calcule un délai intelligent basé sur le contexte"""
        if is_retry:
            # Délai plus long en cas de retry (réduit)
            return base_delay * 1.5 + random.uniform(0.2, 0.8)
        
        # Variation naturelle humaine
        human_variance = random.uniform(-0.1, 0.3)
        
        # Ajout de patterns humains (parfois très rapide, parfois lent)
        if random.random() < 0.2:  # 20% du temps, très rapide
            return base_delay * 0.3 + human_variance
        elif random.random() < 0.08:  # 8% du temps, lent
            return base_delay * 1.3 + human_variance
        
        return base_delay + human_variance
    
    def create_advanced_session(self, use_proxy=False, verify_ssl=True):
        """Crée une session avec configuration avancée"""
        session = requests.Session()
        session.trust_env = False  # Ignore system proxy env (can break crawling)
        
        # Désactiver warnings SSL si nécessaire
        if not verify_ssl:
            import urllib3
            urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
        
        # Configuration retry sophistiquée
        retry = Retry(
            total=5,
            backoff_factor=2,
            status_forcelist=[429, 500, 502, 503, 504, 520, 522, 524],
            allowed_methods=["HEAD", "GET", "OPTIONS", "POST"],
            raise_on_status=False  # Ne pas lever d'exception
        )
        
        adapter = HTTPAdapter(
            max_retries=retry,
            pool_connections=20,
            pool_maxsize=50,
            pool_block=False
        )
        
        session.mount('http://', adapter)
        session.mount('https://', adapter)
        
        # Proxy si disponible
        if use_proxy:
            proxy = self.get_random_proxy()
            if proxy:
                session.proxies.update(proxy)
        
        session.verify = verify_ssl
        
        # Garder les cookies entre requêtes (comportement navigateur)
        session.cookies.update(self.cookies_store.get('default', {}))
        
        return session
    
    def save_cookies(self, session, domain='default'):
        """Sauvegarde les cookies pour réutilisation"""
        self.cookies_store[domain] = session.cookies.get_dict()
    
    @staticmethod
    def normalize_url(url):
        """Normalise une URL pour éviter les doublons"""
        parsed = urlparse(url)
        path = parsed.path or "/"
        
        # Enlever le fragment (#)
        url_without_fragment = url.split('#')[0]
        
        # Trier les paramètres de query pour cohérence
        if parsed.query:
            params = parse_qs(parsed.query)
            sorted_params = sorted(params.items())
            normalized_query = urlencode(sorted_params, doseq=True)
            normalized = f"{parsed.scheme}://{parsed.netloc}{path}?{normalized_query}"
        else:
            normalized = f"{parsed.scheme}://{parsed.netloc}{path}"
        
        # Enlever le trailing slash sauf pour la racine
        if normalized.endswith('/') and parsed.path != '/':
            normalized = normalized[:-1]
        
        return normalized


class AdaptiveRateLimiter:
    """Rate limiter adaptatif qui apprend des réponses du serveur"""
    
    def __init__(self):
        self.domain_timers = {}
        self.domain_delays = defaultdict(lambda: 0.2)  # Délai initial agressif
        self.domain_429_count = defaultdict(int)
        self.lock = threading.Lock()
    
    def wait_if_needed(self, domain, base_delay=2):
        """Attend avec délai adaptatif"""
        with self.lock:
            current_time = time.time()
            
            # Récupérer le délai adaptatif pour ce domaine
            adaptive_delay = self.domain_delays[domain]
            
            if domain in self.domain_timers:
                elapsed = current_time - self.domain_timers[domain]
                if elapsed < adaptive_delay:
                    sleep_time = adaptive_delay - elapsed
                    logger.debug(f"Rate limiting {domain}: {sleep_time:.2f}s")
                    time.sleep(sleep_time)
            
            self.domain_timers[domain] = time.time()
    
    def report_429(self, domain):
        """Signale un rate limit et augmente le délai"""
        with self.lock:
            self.domain_429_count[domain] += 1
            # Augmenter progressivement le délai
            self.domain_delays[domain] = min(
                self.domain_delays[domain] * 1.5,
                30.0  # Max 30 secondes
            )
            logger.warning(f"Rate limit détecté pour {domain}. Nouveau délai: {self.domain_delays[domain]:.1f}s")
    
    def report_success(self, domain):
        """Signale un succès et réduit légèrement le délai"""
        with self.lock:
            if self.domain_delays[domain] > 0.2:
                self.domain_delays[domain] = max(
                    self.domain_delays[domain] * 0.95,
                    0.2  # Min 0.2 seconde
                )


class JavaScriptChallengeSolver:
    """Détecte et tente de résoudre les challenges JavaScript simples"""
    
    @staticmethod
    def detect_challenge(response):
        """Détecte si la réponse contient un challenge JS"""
        indicators = [
            'cloudflare',
            'checking your browser',
            'enable javascript',
            'ddos protection',
            'security check',
            'captcha',
        ]
        
        content_lower = response.text.lower()
        return any(indicator in content_lower for indicator in indicators)
    
    @staticmethod
    def suggest_solutions():
        """Suggère des solutions pour contourner les challenges"""
        return [
            "💡 Ce site utilise une protection anti-bot avancée (Cloudflare/similar)",
            "Solutions possibles:",
            "  1. Utiliser Selenium/Playwright avec un vrai navigateur",
            "  2. Utiliser des services de résolution CAPTCHA",
            "  3. Utiliser l'API officielle du site",
            "  4. Utiliser des proxies résidentiels premium",
        ]


class WebCrawler:
    """Crawler web avec stratégies anti-blocage avancées"""
    
    def __init__(self, mongo_uri="mongodb://localhost:27017/", 
                 db_name="web_crawler_db",
                 use_proxy=False,
                 base_delay=0.2,
                 respect_robots_txt=False,
                 verify_ssl=True,
                 max_retries_per_url=2,
                 request_timeout=12,
                 use_browser_fallback=True,
                 mongo_timeout_ms=2000):
        """Initialise le crawler"""
        try:
            self.mongo_available = False
            self.client = pymongo.MongoClient(mongo_uri, serverSelectionTimeoutMS=mongo_timeout_ms)
            try:
                self.client.admin.command('ping')
                self.mongo_available = True
            except Exception:
                logger.warning("⚠️ MongoDB indisponible, mode sans stockage")
            
            self.db = self.client[db_name] if self.mongo_available else None
            self.sources_collection = self.db['sources'] if self.mongo_available else None
            self.data_collection = self.db['crawled_data'] if self.mongo_available else None
            self.robots_cache = self.db['robots_cache'] if self.mongo_available else None
            self.url_history = self.db['url_history'] if self.mongo_available else None
            
            if self.mongo_available:
                # Index - avec gestion complète des conflits
                try:
                    self.data_collection.create_index([('title', 'text'), ('content', 'text')])
                except:
                    pass
                
                try:
                    self.data_collection.create_index('source_id')
                except:
                    pass
                
                try:
                    self.data_collection.create_index('timestamp')
                except:
                    pass
                
                # Gestion intelligente de l'index URL
                try:
                    # Vérifier si l'index existe déjà
                    existing_indexes = self.data_collection.index_information()
                    if 'url_1' in existing_indexes:
                        # Si l'index existe sans unique, le supprimer et recréer
                        current_index = existing_indexes['url_1']
                        if not current_index.get('unique', False):
                            logger.info("🔄 Recréation de l'index URL avec contrainte unique...")
                            self.data_collection.drop_index('url_1')
                            self.data_collection.create_index('url', unique=True, sparse=True, name='url_unique_idx')
                        # Sinon l'index existe déjà correctement
                    else:
                        # Créer l'index
                        self.data_collection.create_index('url', unique=True, sparse=True, name='url_unique_idx')
                except pymongo.errors.DuplicateKeyError:
                    logger.warning("⚠️  Doublons détectés, index URL sans contrainte unique")
                    try:
                        self.data_collection.drop_index('url_1')
                    except:
                        pass
                    try:
                        self.data_collection.drop_index('url_unique_idx')
                    except:
                        pass
                    self.data_collection.create_index('url', name='url_idx')
                except Exception as e:
                    logger.warning(f"⚠️  Index URL non créé: {e}")
                
                # Index pour url_history
                try:
                    existing_history_indexes = self.url_history.index_information()
                    if 'url_1' not in existing_history_indexes:
                        self.url_history.create_index('url', unique=True, sparse=True)
                except:
                    pass
                
                try:
                    self.url_history.create_index('last_crawled')
                except:
                    pass
            
            # Configuration
            self.use_proxy = use_proxy
            self.base_delay = base_delay
            self.respect_robots_txt = respect_robots_txt
            self.verify_ssl = verify_ssl
            self.max_retries_per_url = max_retries_per_url
            self.request_timeout = request_timeout
            self.use_browser_fallback = use_browser_fallback
            
            # Stratégies anti-blocage
            self.rate_limiter = AdaptiveRateLimiter()
            self.anti_blocking = AdvancedAntiBlockingStrategy()
            self.js_solver = JavaScriptChallengeSolver()
            
            if self.mongo_available:
                logger.info(f"✓ MongoDB: {db_name}")
            logger.info(f"✓ Config: proxy={use_proxy}, delay={base_delay}s, SSL={verify_ssl}")
            logger.info(f"✓ Stratégies avancées activées")
        except Exception as e:
            logger.error(f"Erreur MongoDB: {e}")
            raise

    @staticmethod
    def _expand_keywords(keywords):
        expanded = set(keywords)
        for kw in list(expanded):
            if kw == "education":
                expanded.update([
                    "educational",
                    "school",
                    "schools",
                    "student",
                    "students",
                    "teacher",
                    "teachers",
                    "university",
                    "universities",
                    "college",
                    "campus",
                    "classroom",
                    "curriculum",
                    "exam",
                    "exams",
                    "scholarship",
                    "education ministry",
                    "ministry of education",
                    "education system",
                    "enseignement",
                    "ecole",
                    "ecoles",
                    "universite",
                    "universites",
                    "etudiant",
                    "etudiants",
                    "professeur",
                    "professeurs",
                    "formation",
                    "scolarite",
                    "lycee",
                    "bac",
                    "baccalaureat",
                    "التعليم",
                    "مدرسة",
                    "مدارس",
                    "جامعة",
                    "جامعات",
                    "طالب",
                    "طلاب",
                    "تلميذ",
                    "تلاميذ",
                    "أستاذ",
                    "أساتذة",
                    "امتحان",
                    "امتحانات",
                    "وزارة التربية",
                    "التعليم العالي",
                ])
            elif kw == "finance":
                expanded.update([
                    "financial",
                    "economy",
                    "economic",
                    "bank",
                    "banks",
                    "banking",
                    "investment",
                    "investments",
                    "stock",
                    "stocks",
                    "market",
                    "markets",
                    "bond",
                    "bonds",
                    "inflation",
                    "budget",
                    "tax",
                    "taxes",
                    "loan",
                    "loans",
                    "credit",
                    "currency",
                    "currencies",
                    "fund",
                    "funds",
                    "finance ministry",
                    "ministry of finance",
                    "économie",
                    "économique",
                    "banque",
                    "banques",
                    "bourse",
                    "marché",
                    "marchés",
                    "investissement",
                    "investissements",
                    "inflation",
                    "budget",
                    "impôt",
                    "impôts",
                    "crédit",
                    "monnaie",
                    "finances",
                    "تمويل",
                    "مالي",
                    "مالية",
                    "اقتصاد",
                    "اقتصادي",
                    "بنك",
                    "بنوك",
                    "استثمار",
                    "استثمارات",
                    "بورصة",
                    "سوق",
                    "أسواق",
                    "تضخم",
                    "ميزانية",
                    "ضرائب",
                    "قرض",
                    "قروض",
                    "وزارة المالية",
                ])
            elif kw == "health":
                expanded.update([
                    "healthcare",
                    "medical",
                    "medicine",
                    "doctor",
                    "doctors",
                    "hospital",
                    "hospitals",
                    "clinic",
                    "clinics",
                    "patient",
                    "patients",
                    "public health",
                    "vaccine",
                    "vaccines",
                    "epidemic",
                    "pandemic",
                    "disease",
                    "diseases",
                    "treatment",
                    "pharmacy",
                    "pharmacies",
                    "ministry of health",
                    "santé",
                    "sanitaire",
                    "médical",
                    "médecine",
                    "hôpital",
                    "hôpitaux",
                    "clinique",
                    "cliniques",
                    "patient",
                    "patients",
                    "vaccin",
                    "vaccins",
                    "épidémie",
                    "pandémie",
                    "maladie",
                    "maladies",
                    "traitement",
                    "pharmacie",
                    "pharmacies",
                    "وزارة الصحة",
                    "صحة",
                    "صحي",
                    "طبيب",
                    "أطباء",
                    "مستشفى",
                    "مستشفيات",
                    "عيادة",
                    "عيادات",
                    "مريض",
                    "مرضى",
                    "لقاح",
                    "لقاحات",
                    "وباء",
                    "جائحة",
                    "مرض",
                    "أمراض",
                    "علاج",
                    "صيدلية",
                    "صيدليات",
                ])
        return list(expanded)

    @staticmethod
    def _normalize_text(text):
        text = (text or "").lower()
        text = WebCrawler._normalize_arabic(text)
        return " ".join(text.split())

    @staticmethod
    def _normalize_arabic(text):
        if not text:
            return ""
        # Normalize common Arabic letter variants and strip diacritics
        replacements = {
            "أ": "ا",
            "إ": "ا",
            "آ": "ا",
            "ى": "ي",
            "ؤ": "و",
            "ئ": "ي",
            "ة": "ه",
            "ٱ": "ا",
        }
        for src, dst in replacements.items():
            text = text.replace(src, dst)
        text = re.sub(r"[\u064B-\u065F\u0670\u06D6-\u06ED]", "", text)
        return text

    @staticmethod
    def _keyword_in_text(text, keyword):
        if not text or not keyword:
            return False
        text = WebCrawler._normalize_text(text)
        keyword = WebCrawler._normalize_text(keyword)
        if " " in keyword:
            return keyword in text
        pattern = r"(?<!\\w)" + re.escape(keyword) + r"(?!\\w)"
        return re.search(pattern, text, flags=re.UNICODE) is not None

    def _link_is_relevant(self, link_text, link_url, keywords):
        if not keywords:
            return True
        haystack = f"{link_text} {link_url}"
        return any(self._keyword_in_text(haystack, kw) for kw in keywords)

    @staticmethod
    def _looks_like_listing(url):
        try:
            path = urlparse(url).path or "/"
        except Exception:
            return False
        if path in ["", "/"]:
            return True
        if path.endswith("/"):
            return True
        last_segment = path.rsplit("/", 1)[-1]
        return "." not in last_segment

    def _extract_main_text(self, soup):
        candidates = []
        for tag in ["article", "main"]:
            node = soup.find(tag)
            if node:
                text = node.get_text(separator=" ", strip=True)
                if len(text) >= 200:
                    return text
                candidates.append(text)

        for selector in [
            ".post-content",
            ".article-content",
            ".entry-content",
            ".post",
            ".content",
            "#content",
            ".single-content",
            ".story",
        ]:
            node = soup.select_one(selector)
            if node:
                text = node.get_text(separator=" ", strip=True)
                if len(text) >= 200:
                    return text
                candidates.append(text)

        if candidates:
            return max(candidates, key=len)
        return soup.get_text(separator=" ", strip=True)
    
    def check_robots_txt(self, url):
        """Vérifie robots.txt"""
        if not self.respect_robots_txt:
            return True
        
        try:
            from urllib.robotparser import RobotFileParser
            parsed = urlparse(url)
            robots_url = f"{parsed.scheme}://{parsed.netloc}/robots.txt"
            
            cached = self.robots_cache.find_one({'url': robots_url})
            if cached and (datetime.now() - cached['timestamp']).days < 7:
                return cached['allowed']
            
            rp = RobotFileParser()
            rp.set_url(robots_url)
            try:
                rp.read()
                allowed = rp.can_fetch("*", url)
                
                self.robots_cache.update_one(
                    {'url': robots_url},
                    {'$set': {'allowed': allowed, 'timestamp': datetime.now()}},
                    upsert=True
                )
                return allowed
            except:
                return True
        except Exception as e:
            logger.warning(f"Erreur robots.txt: {e}")
            return True
    
    def is_url_recently_crawled(self, url, hours=24):
        """Vérifie si l'URL a été crawlée récemment"""
        if not self.mongo_available:
            return False
        recent = self.url_history.find_one({
            'url': url,
            'last_crawled': {'$gte': datetime.now() - timedelta(hours=hours)}
        })
        return recent is not None
    
    def mark_url_crawled(self, url, success=True):
        """Marque une URL comme crawlée"""
        if not self.mongo_available:
            return
        self.url_history.update_one(
            {'url': url},
            {
                '$set': {
                    'last_crawled': datetime.now(),
                    'success': success
                },
                '$inc': {'crawl_count': 1}
            },
            upsert=True
        )
    
    def add_source(self, url, source_type='website',
                   frequency='daily', schedule_time='09:00',
                   max_hits=100, content_types=None, keywords=None,
                   enabled=True):
        """Ajoute une source"""
        if content_types is None:
            content_types = ['html', 'text']
        if keywords is None:
            keywords = []
        
        source = {
            'url': url,
            'type': source_type,
            'frequency': frequency,
            'schedule_time': schedule_time,
            'max_hits': max_hits,
            'content_types': content_types,
            'keywords': keywords,
            'enabled': enabled,
            'last_crawl': None,
            'status': 'pending',
            'created_at': datetime.now(),
            'failed_attempts': 0,
            'success_count': 0
        }
        
        result = self.sources_collection.insert_one(source)
        logger.info(f"Source ajoutée: {url}")
        return str(result.inserted_id)
    
    def get_sources(self, enabled_only=False):
        """Récupère les sources"""
        query = {'enabled': True} if enabled_only else {}
        sources = list(self.sources_collection.find(query))
        for source in sources:
            source['_id'] = str(source['_id'])
        return sources
    
    def delete_source(self, source_id):
        """Supprime une source"""
        try:
            from bson.objectid import ObjectId
            self.data_collection.delete_many({'source_id': source_id})
            result = self.sources_collection.delete_one({'_id': ObjectId(source_id)})
            logger.info(f"Source supprimée: {source_id}")
            return result.deleted_count > 0
        except Exception as e:
            logger.error(f"Erreur suppression: {e}")
            return False
    
    def crawl_url(self, url, content_types, max_hits=100, control=None, stats_cb=None, keywords=None, skip_recent=True, prefer_browser=False):
        """Crawl avec stratégies anti-blocage avancées"""
        normalized_types = [ct.lower().strip() for ct in (content_types or [])]
        if "rss" in normalized_types and "xml" not in normalized_types:
            normalized_types.append("xml")
        content_types = normalized_types or ["html"]
        keywords = [k.strip().lower() for k in (keywords or []) if k.strip()]
        keywords = self._expand_keywords(keywords)
        browser_fetcher = None
        first_fetch = True

        def try_browser_fetch(target_url):
            nonlocal browser_fetcher
            if not self.use_browser_fallback:
                return None
            if browser_fetcher is None:
                from crawler.browser_fetcher import BrowserFetcher
                browser_fetcher = BrowserFetcher()
            if stats_cb:
                stats_cb("error", {"url": target_url, "error": "Using browser fallback"})
            return browser_fetcher.fetch(target_url, timeout_sec=self.request_timeout)

        def extract_links(html_bytes, current_url, depth):
            try:
                soup = BeautifulSoup(html_bytes, 'html.parser')
                links_found = 0
                allow_first_hop = depth == 0
                listing_candidates = []
                for link in soup.find_all('a', href=True):
                    absolute_url = urljoin(current_url, link['href'])
                    clean_url = self.anti_blocking.normalize_url(absolute_url)
                    link_text = link.get_text(separator=" ", strip=True)
                    if keywords and not allow_first_hop:
                        if not self._link_is_relevant(link_text, clean_url, keywords):
                            continue
                    elif keywords and allow_first_hop:
                        if self._looks_like_listing(clean_url):
                            listing_candidates.append(clean_url)
                    if self._is_same_domain(url, clean_url):
                        if clean_url not in visited_urls and clean_url not in [f[0] for f in failed_urls]:
                            if clean_url not in [u for u, _ in urls_to_visit]:
                                urls_to_visit.append((clean_url, depth + 1))
                                links_found += 1
                if keywords and allow_first_hop and links_found == 0:
                    for candidate in listing_candidates[:10]:
                        if candidate not in visited_urls and candidate not in [f[0] for f in failed_urls]:
                            if candidate not in [u for u, _ in urls_to_visit]:
                                urls_to_visit.append((candidate, depth + 1))
                                links_found += 1
                if links_found > 0:
                    logger.info(f"   ?+' {links_found} nouveaux liens")
            except Exception:
                pass
        
        def should_stop():
            if control is None:
                return False
            stop_event = getattr(control, "stop_event", None)
            if stop_event is None and isinstance(control, dict):
                stop_event = control.get("stop_event")
            return bool(stop_event and stop_event.is_set())

        def wait_if_paused():
            if control is None:
                return
            pause_event = getattr(control, "pause_event", None)
            if pause_event is None and isinstance(control, dict):
                pause_event = control.get("pause_event")
            if pause_event is not None:
                pause_event.wait()

        if stats_cb:
            stats_cb("start", {"url": url, "max_hits": max_hits})

        collected_data = []
        visited_urls = set()
        urls_to_visit = [(url, 0)]
        failed_urls = {}  # URL -> (retry_count, last_error)
        
        session = self.anti_blocking.create_advanced_session(
            use_proxy=self.use_proxy,
            verify_ssl=self.verify_ssl
        )
        
        domain = urlparse(url).netloc
        last_referer = None
        
        while urls_to_visit and len(collected_data) < max_hits:
            if should_stop():
                if stats_cb:
                    stats_cb("stopped", {"url": url})
                break

            wait_if_paused()

            current_url, depth = urls_to_visit.pop(0)
            normalized_url = self.anti_blocking.normalize_url(current_url)

            if stats_cb:
                stats_cb("attempt", {"url": current_url, "queue": len(urls_to_visit)})
            
            if normalized_url in visited_urls:
                continue
            
            # Vérifier retry count
            if normalized_url in failed_urls:
                retry_count, _ = failed_urls[normalized_url]
                if retry_count >= self.max_retries_per_url:
                    logger.debug(f"Abandonné après {retry_count} tentatives: {current_url}")
                    continue
            
            # Robots.txt
            if not self.check_robots_txt(current_url):
                logger.info(f"⛔ Bloqué par robots.txt: {current_url}")
                failed_urls[normalized_url] = (999, "robots.txt")
                if stats_cb:
                    stats_cb("error", {"url": current_url, "error": "Blocked by robots.txt"})
                continue
            
            # Éviter de re-crawler trop vite
            if skip_recent and self.is_url_recently_crawled(normalized_url, hours=1):
                logger.debug(f"Déjà crawlé récemment: {current_url}")
                if stats_cb:
                    stats_cb("error", {"url": current_url, "error": "Recently crawled (1h)"})
                continue
            
            visited_urls.add(normalized_url)
            
            # Optionnel: navigateur en premier sur le tout premier fetch
            if prefer_browser and first_fetch:
                fallback = try_browser_fetch(current_url)
                first_fetch = False
                if fallback:
                    html, final_url, method = fallback
                    data = self._process_html(final_url, html)
                    if data and self._is_relevant(data, keywords):
                        collected_data.append(data)
                        self.mark_url_crawled(normalized_url, success=True)
                        if len(collected_data) < max_hits:
                            extract_links(html, final_url, depth)
                        if stats_cb:
                            stats_cb("success", {"url": current_url, "content_type": "html", "method": method})
                        continue
                    elif data and stats_cb:
                        if len(collected_data) < max_hits:
                            extract_links(html, final_url, depth)
                        stats_cb("error", {"url": current_url, "error": "Filtered by keywords"})

            # Rate limiting adaptatif
            is_retry = normalized_url in failed_urls
            delay = self.anti_blocking.calculate_intelligent_delay(
                self.base_delay, 
                domain, 
                is_retry
            )
            self.rate_limiter.wait_if_needed(domain, delay)
            
            try:
                logger.info(f"🔍 Crawl: {current_url}")
                
                # Headers avancés avec referer intelligent
                headers = self.anti_blocking.get_advanced_headers(
                    url=current_url,
                    referer=last_referer
                )
                
                response = session.get(
                    current_url,
                    headers=headers,
                    timeout=self.request_timeout,
                    allow_redirects=True
                )

                # Détecter challenge JS même avec status 200
                if self.use_browser_fallback and self.js_solver.detect_challenge(response):
                    try:
                        soup = BeautifulSoup(response.content, 'html.parser')
                        link_count = len(soup.find_all('a', href=True))
                    except Exception:
                        link_count = 0

                    if link_count < 5:
                        fallback = try_browser_fetch(current_url)
                        if fallback:
                            html, final_url, method = fallback
                            data = self._process_html(final_url, html)
                            if data and self._is_relevant(data, keywords):
                                collected_data.append(data)
                                self.mark_url_crawled(normalized_url, success=True)
                                if len(collected_data) < max_hits:
                                    extract_links(html, final_url, depth)
                                if stats_cb:
                                    stats_cb("success", {"url": current_url, "content_type": "html", "method": method})
                                continue
                            elif data and stats_cb:
                                if len(collected_data) < max_hits:
                                    extract_links(html, final_url, depth)
                                stats_cb("error", {"url": current_url, "error": "Filtered by keywords"})
                
                # Gestion des codes d'erreur
                if response.status_code == 429:
                    logger.warning(f"⏱️  429 Rate Limited: {current_url}")
                    self.rate_limiter.report_429(domain)
                    retry_after = int(response.headers.get('Retry-After', 60))
                    if control is not None:
                        retry_after = min(retry_after, 10)
                    logger.info(f"Attente de {retry_after}s...")
                    if stats_cb:
                        stats_cb("error", {"url": current_url, "error": f"Rate limited (retry {retry_after}s)"})
                    time.sleep(retry_after)
                    urls_to_visit.insert(0, (current_url, depth))
                    visited_urls.remove(normalized_url)
                    continue
                
                if response.status_code in [401, 403]:
                    logger.warning(f"🚫 {response.status_code} Accès refusé: {current_url}")
                    
                    # Détecter challenge JavaScript
                    if self.js_solver.detect_challenge(response):
                        logger.warning("⚠️  Protection anti-bot détectée!")
                        for msg in self.js_solver.suggest_solutions():
                            logger.info(msg)
                    
                    if self.use_browser_fallback:
                        fallback = try_browser_fetch(current_url)
                        if fallback:
                            html, final_url, method = fallback
                            data = self._process_html(final_url, html)
                            if data and self._is_relevant(data, keywords):
                                collected_data.append(data)
                                self.mark_url_crawled(normalized_url, success=True)
                                if len(collected_data) < max_hits:
                                    extract_links(html, final_url, depth)
                                if stats_cb:
                                    stats_cb("success", {"url": current_url, "content_type": "html", "method": method})
                                continue
                            elif data and stats_cb:
                                if len(collected_data) < max_hits:
                                    extract_links(html, final_url, depth)
                                stats_cb("error", {"url": current_url, "error": "Filtered by keywords"})

                    failed_urls[normalized_url] = (
                        failed_urls.get(normalized_url, (0, ""))[0] + 1,
                        f"HTTP {response.status_code}"
                    )
                    if stats_cb:
                        stats_cb("error", {"url": current_url, "error": f"HTTP {response.status_code}"})
                    time.sleep(5)
                    continue
                
                response.raise_for_status()
                
                # Sauvegarder cookies
                self.anti_blocking.save_cookies(session, domain)
                
                # Succès: reporter au rate limiter
                self.rate_limiter.report_success(domain)
                
                # Traiter le contenu
                content_type = response.headers.get('Content-Type', '').lower()
                data = None
                
                data = None

                if 'html' in content_type and 'html' in content_types:
                    data = self._process_html(current_url, response.content)
                    if data:
                        logger.info(f"Fetched: {data['title'][:60]}")
                        
                        # Extraire liens si besoin
                        if len(collected_data) < max_hits:
                            extract_links(response.content, current_url, depth)
                        
                        last_referer = current_url
                
                elif 'xml' in content_type and 'xml' in content_types:
                    data = self._process_xml(current_url, response.content)
                    if data:
                        logger.info(f"Fetched XML: {data['title'][:60]}")
                
                elif 'pdf' in content_type and 'pdf' in content_types:
                    data = self._process_pdf(current_url, response.content)
                    if data:
                        logger.info(f"Fetched PDF: {data['title'][:60]}")
                
                elif 'text' in content_type and 'text' in content_types:
                    data = self._process_text(current_url, response.text)
                    if data:
                        logger.info(f"Fetched text: {data['title'][:60]}")
                
                else:
                    # Essayer HTML par défaut
                    if 'html' in content_types:
                        data = self._process_html(current_url, response.content)
                        if data:
                            logger.info(f"Fetched page: {data['title'][:60]}")
                
                if data and self._is_relevant(data, keywords):
                    collected_data.append(data)
                    self.mark_url_crawled(normalized_url, success=True)
                    if stats_cb:
                        stats_cb("success", {"url": current_url, "content_type": content_type})
                elif data and stats_cb:
                    stats_cb("error", {"url": current_url, "error": "Filtered by keywords"})
                
            except requests.exceptions.Timeout:
                logger.warning(f"⏱️  Timeout: {current_url}")
                if self.use_browser_fallback:
                    fallback = try_browser_fetch(current_url)
                    if fallback:
                        html, final_url, method = fallback
                        data = self._process_html(final_url, html)
                        if data and self._is_relevant(data, keywords):
                            collected_data.append(data)
                            self.mark_url_crawled(normalized_url, success=True)
                            if len(collected_data) < max_hits:
                                extract_links(html, final_url, depth)
                            if stats_cb:
                                stats_cb("success", {"url": current_url, "content_type": "html", "method": method})
                            continue
                        elif data and stats_cb:
                            if len(collected_data) < max_hits:
                                extract_links(html, final_url, depth)
                            stats_cb("error", {"url": current_url, "error": "Filtered by keywords"})
                failed_urls[normalized_url] = (
                    failed_urls.get(normalized_url, (0, ""))[0] + 1,
                    "Timeout"
                )
                if stats_cb:
                    stats_cb("error", {"url": current_url, "error": "Timeout"})
                
            except requests.exceptions.ConnectionError as e:
                logger.warning(f"🔌 Erreur connexion: {current_url}")
                if self.use_browser_fallback:
                    fallback = try_browser_fetch(current_url)
                    if fallback:
                        html, final_url, method = fallback
                        data = self._process_html(final_url, html)
                        if data and self._is_relevant(data, keywords):
                            collected_data.append(data)
                            self.mark_url_crawled(normalized_url, success=True)
                            if len(collected_data) < max_hits:
                                extract_links(html, final_url, depth)
                            if stats_cb:
                                stats_cb("success", {"url": current_url, "content_type": "html", "method": method})
                            continue
                        elif data and stats_cb:
                            if len(collected_data) < max_hits:
                                extract_links(html, final_url, depth)
                            stats_cb("error", {"url": current_url, "error": "Filtered by keywords"})
                failed_urls[normalized_url] = (
                    failed_urls.get(normalized_url, (0, ""))[0] + 1,
                    "Connection Error"
                )
                time.sleep(5)
                if stats_cb:
                    stats_cb("error", {"url": current_url, "error": "Connection Error"})
                
            except requests.exceptions.TooManyRedirects:
                logger.warning(f"🔄 Trop de redirections: {current_url}")
                failed_urls[normalized_url] = (999, "Too Many Redirects")
                if stats_cb:
                    stats_cb("error", {"url": current_url, "error": "Too Many Redirects"})
                
            except Exception as e:
                logger.warning(f"❌ Erreur: {current_url} - {str(e)[:100]}")
                failed_urls[normalized_url] = (
                    failed_urls.get(normalized_url, (0, ""))[0] + 1,
                    str(e)[:100]
                )
                if stats_cb:
                    stats_cb("error", {"url": current_url, "error": str(e)[:100]})
        
        session.close()
        logger.info(f"📊 Résumé: {len(collected_data)} pages collectées, {len(failed_urls)} échecs")

        if stats_cb:
            stats_cb("done", {"collected": len(collected_data), "failed": len(failed_urls)})
        
        return collected_data
    
    def _is_same_domain(self, base_url, check_url):
        """Vérifie si même domaine"""
        base = urlparse(base_url).netloc.lower()
        check = urlparse(check_url).netloc.lower()
        base = base.replace("www.", "")
        check = check.replace("www.", "")
        if base == check:
            return True
        return check.endswith("." + base) or base.endswith("." + check)
    
    def _process_html(self, url, content):
        """Traite HTML"""
        try:
            soup = BeautifulSoup(content, 'html.parser')
            
            for script in soup(['script', 'style', 'nav', 'footer', 'aside', 'header']):
                script.decompose()
            
            title = soup.title.string if soup.title else 'Sans titre'
            title = title.strip()[:200]
            
            text_content = self._extract_main_text(soup)
            
            keywords = []
            meta_keywords = soup.find('meta', attrs={'name': 'keywords'})
            if meta_keywords and meta_keywords.get('content'):
                keywords = [k.strip() for k in meta_keywords['content'].split(',')][:10]
            
            description = ''
            meta_desc = soup.find('meta', attrs={'name': 'description'})
            if meta_desc and meta_desc.get('content'):
                description = meta_desc['content'][:500]
            
            return {
                'url': url,
                'title': title,
                'description': description,
                'content': text_content[:10000],
                'content_type': 'html',
                'keywords': keywords,
                'timestamp': datetime.now()
            }
        except Exception as e:
            logger.error(f"Erreur HTML: {e}")
            return None
    
    def _process_xml(self, url, content):
        """Traite XML/RSS"""
        try:
            soup = BeautifulSoup(content, 'xml')
            items = soup.find_all('item')
            if items:
                item = items[0]
                title = item.find('title').text if item.find('title') else 'Sans titre'
                description = item.find('description').text if item.find('description') else ''
                
                return {
                    'url': url,
                    'title': title,
                    'description': description[:500],
                    'content': description[:5000],
                    'content_type': 'xml',
                    'keywords': [],
                    'timestamp': datetime.now()
                }
            return None
        except Exception as e:
            logger.error(f"Erreur XML: {e}")
            return None
    
    def _process_pdf(self, url, content):
        """Traite PDF"""
        try:
            pdf_file = io.BytesIO(content)
            text_content = ""
            
            with pdfplumber.open(pdf_file) as pdf:
                for page in pdf.pages[:10]:
                    text_content += page.extract_text() or ""
            
            return {
                'url': url,
                'title': url.split('/')[-1],
                'description': text_content[:500],
                'content': text_content[:10000],
                'content_type': 'pdf',
                'keywords': [],
                'timestamp': datetime.now()
            }
        except Exception as e:
            logger.error(f"Erreur PDF: {e}")
            return None
    
    def _process_text(self, url, content):
        """Traite texte brut"""
        try:
            return {
                'url': url,
                'title': url.split('/')[-1],
                'description': content[:500],
                'content': content[:10000],
                'content_type': 'text',
                'keywords': [],
                'timestamp': datetime.now()
            }
        except Exception as e:
            logger.error(f"Erreur texte: {e}")
            return None

    def _is_relevant(self, data, keywords):
        if not keywords:
            return True

        title = str(data.get('title', ''))
        description = str(data.get('description', ''))
        content = str(data.get('content', ''))
        url = str(data.get('url', ''))
        meta_keywords = " ".join(data.get('keywords', []) or [])

        strict_finance = "finance" in keywords
        strict_health = "health" in keywords
        strict_mode = strict_finance or strict_health

        finance_terms = {
            "finance", "financial", "economy", "economic", "bank", "banks", "banking",
            "investment", "investments", "stock", "stocks", "market", "markets",
            "bond", "bonds", "inflation", "budget", "tax", "taxes", "loan", "loans",
            "credit", "currency", "currencies", "fund", "funds", "finance ministry",
            "ministry of finance", "économie", "économique", "banque", "banques",
            "bourse", "marché", "marchés", "investissement", "investissements",
            "impôt", "impôts", "crédit", "monnaie", "finances", "تمويل", "مالي",
            "مالية", "اقتصاد", "اقتصادي", "بنك", "بنوك", "استثمار", "استثمارات",
            "بورصة", "سوق", "أسواق", "تضخم", "ميزانية", "ضرائب", "قرض", "قروض",
            "وزارة المالية",
        }
        health_terms = {
            "health", "healthcare", "medical", "medicine", "doctor", "doctors",
            "hospital", "hospitals", "clinic", "clinics", "patient", "patients",
            "public health", "vaccine", "vaccines", "epidemic", "pandemic",
            "disease", "diseases", "treatment", "pharmacy", "pharmacies",
            "ministry of health", "santé", "sanitaire", "médical", "médecine",
            "hôpital", "hôpitaux", "clinique", "cliniques", "vaccin", "vaccins",
            "épidémie", "pandémie", "maladie", "maladies", "traitement",
            "pharmacie", "pharmacies", "وزارة الصحة", "صحة", "صحي", "طبيب",
            "أطباء", "مستشفى", "مستشفيات", "عيادة", "عيادات", "مريض", "مرضى",
            "لقاح", "لقاحات", "وباء", "جائحة", "مرض", "أمراض", "علاج",
            "صيدلية", "صيدليات",
        }
        precision_terms = set()
        if strict_finance:
            precision_terms.update(finance_terms)
        if strict_health:
            precision_terms.update(health_terms)

        title_match = any(self._keyword_in_text(title, kw) for kw in keywords)
        description_match = any(self._keyword_in_text(description, kw) for kw in keywords)
        url_match = any(self._keyword_in_text(url, kw) for kw in keywords)
        meta_match = any(self._keyword_in_text(meta_keywords, kw) for kw in keywords)

        if title_match or description_match or url_match or meta_match:
            if not strict_mode:
                return True
            high_precision = any(
                self._keyword_in_text(title, kw)
                or self._keyword_in_text(description, kw)
                or self._keyword_in_text(url, kw)
                or self._keyword_in_text(meta_keywords, kw)
                for kw in precision_terms
            )
            if high_precision:
                return True
        elif strict_mode:
            high_precision = any(
                self._keyword_in_text(title, kw)
                or self._keyword_in_text(description, kw)
                or self._keyword_in_text(url, kw)
                or self._keyword_in_text(meta_keywords, kw)
                for kw in precision_terms
            )
            if not high_precision:
                return False

        if self._looks_like_listing(url):
            return False

        normalized_content = self._normalize_text(content)
        if len(normalized_content) < 300:
            return False

        content_matches = {kw for kw in keywords if self._keyword_in_text(normalized_content, kw)}
        if not content_matches:
            return False

        if strict_mode:
            precision_hits = {kw for kw in precision_terms if self._keyword_in_text(normalized_content, kw)}
            return len(content_matches) >= 2 and len(precision_hits) >= 1

        content_hits = 0
        for kw in content_matches:
            if " " in kw:
                if kw in normalized_content:
                    content_hits += 2
            else:
                matches = re.findall(r"(?<!\\w)" + re.escape(kw) + r"(?!\\w)", normalized_content, flags=re.UNICODE)
                content_hits += min(len(matches), 3)

        return content_hits >= 3
    
    def crawl_source(self, source_id):
        """Crawl une source"""
        try:
            from bson.objectid import ObjectId
            source = self.sources_collection.find_one({'_id': ObjectId(source_id)})
            
            if not source or not source.get('enabled'):
                logger.warning(f"Source {source_id} introuvable ou désactivée")
                return 0
            
            logger.info(f"🚀 Début crawl: {source['url']}")
            
            self.sources_collection.update_one(
                {'_id': ObjectId(source_id)},
                {'$set': {'status': 'crawling'}}
            )
            
            collected_data = self.crawl_url(
                source['url'],
                source['content_types'],
                source['max_hits'],
                keywords=source.get('keywords', [])
            )
            
            count = 0
            for data in collected_data:
                data['source_id'] = source_id
                try:
                    self.data_collection.insert_one(data)
                    count += 1
                except pymongo.errors.DuplicateKeyError:
                    logger.debug(f"Doublon ignoré: {data['url']}")
            
            self.sources_collection.update_one(
                {'_id': ObjectId(source_id)},
                {
                    '$set': {
                        'status': 'completed',
                        'last_crawl': datetime.now(),
                        'failed_attempts': 0
                    },
                    '$inc': {'success_count': 1}
                }
            )
            
            logger.info(f"✅ Crawl terminé: {count} éléments sauvegardés")
            return count
            
        except Exception as e:
            logger.error(f"❌ Erreur crawl: {e}")
            
            try:
                from bson.objectid import ObjectId
                self.sources_collection.update_one(
                    {'_id': ObjectId(source_id)},
                    {
                        '$set': {'status': 'failed'},
                        '$inc': {'failed_attempts': 1}
                    }
                )
                
                source = self.sources_collection.find_one({'_id': ObjectId(source_id)})
                if source and source.get('failed_attempts', 0) >= 5:
                    self.sources_collection.update_one(
                        {'_id': ObjectId(source_id)},
                        {'$set': {'enabled': False, 'status': 'disabled_after_failures'}}
                    )
                    logger.warning(f"⚠️  Source {source_id} désactivée après 5 échecs")
            except:
                pass
            
            return 0
    
    def search_data(self, query, limit=50):
        """Recherche par mots-clés"""
        try:
            results = list(self.data_collection.find(
                {'$text': {'$search': query}},
                {'score': {'$meta': 'textScore'}}
            ).sort([('score', {'$meta': 'textScore'})]).limit(limit))
            
            for result in results:
                result['_id'] = str(result['_id'])
            
            logger.info(f"🔍 Recherche '{query}': {len(results)} résultats")
            return results
            
        except Exception as e:
            logger.error(f"Erreur recherche: {e}")
            return []
    
    def get_statistics(self):
        """Statistiques"""
        return {
            'total_sources': self.sources_collection.count_documents({}),
            'active_sources': self.sources_collection.count_documents({'enabled': True}),
            'failed_sources': self.sources_collection.count_documents({'status': 'failed'}),
            'total_data': self.data_collection.count_documents({}),
            'urls_crawled': self.url_history.count_documents({}),
            'last_update': datetime.now()
        }
    
    def schedule_crawls(self):
        """Planificateur"""
        sources = self.get_sources(enabled_only=True)
        
        for source in sources:
            source_id = source['_id']
            frequency = source['frequency']
            schedule_time = source.get('schedule_time', '09:00')
            
            if frequency == 'hourly':
                schedule.every().hour.do(self.crawl_source, source_id)
            elif frequency == 'daily':
                schedule.every().day.at(schedule_time).do(self.crawl_source, source_id)
            elif frequency == 'weekly':
                schedule.every().week.at(schedule_time).do(self.crawl_source, source_id)
            elif frequency == 'monthly':
                schedule.every(30).days.at(schedule_time).do(self.crawl_source, source_id)
        
        logger.info(f"⏰ Planificateur: {len(sources)} sources")
        
        def run_scheduler():
            while True:
                schedule.run_pending()
                time.sleep(60)
        
        scheduler_thread = threading.Thread(target=run_scheduler, daemon=True)
        scheduler_thread.start()
        logger.info("✓ Planificateur démarré")
    
    def close(self):
        """Ferme MongoDB"""
        self.client.close()
        logger.info("✓ Connexion fermée")


def main():
    """Interface console"""
    print("=" * 70)
    print("    WEB CRAWLER AVANCÉ - STRATÉGIES ANTI-BLOCAGE    ")
    print("=" * 70)
    print("\n🎯 Nouvelles fonctionnalités anti-blocage:")
    print("  • Rate limiting adaptatif (apprend des réponses)")
    print("  • Headers avancés avec fingerprinting")
    print("  • Gestion intelligente des cookies")
    print("  • Normalisation d'URLs anti-doublons")
    print("  • Détection de challenges JavaScript")
    print("  • Retry intelligent avec backoff")
    print("  • Historique d'URLs pour éviter re-crawl")
    print("\n⚙️  Configuration:")
    
    use_proxy = input("Proxies? (o/n) [n]: ").strip().lower() == 'o'
    
    if use_proxy and not AdvancedAntiBlockingStrategy.PROXIES:
        print("\n⚠️  Aucun proxy configuré!")
        if input("Continuer sans proxy? (o/n): ").strip().lower() != 'o':
            use_proxy = False
    
    base_delay = float(input("Délai de base (secondes) [2]: ").strip() or '2')
    respect_robots = input("Respecter robots.txt? (o/n) [o]: ").strip().lower() != 'n'
    verify_ssl = input("Vérifier SSL? (o/n) [o]: ").strip().lower() != 'n'
    max_retries = int(input("Max tentatives par URL [3]: ").strip() or '3')
    
    crawler = WebCrawler(
        use_proxy=use_proxy,
        base_delay=base_delay,
        respect_robots_txt=respect_robots,
        verify_ssl=verify_ssl,
        max_retries_per_url=max_retries
    )
    
    while True:
        print("\n" + "="*70)
        print("MENU PRINCIPAL")
        print("="*70)
        print("1. ➕ Ajouter une source")
        print("2. 📋 Lister les sources")
        print("3. 🔍 Crawler une source")
        print("4. 🚀 Crawler toutes les sources actives")
        print("5. 🔎 Rechercher dans les données")
        print("6. 📊 Statistiques")
        print("7. 🗑️  Supprimer une source")
        print("8. ⏰ Démarrer le planificateur")
        print("9. 🚪 Quitter")
        
        choice = input("\nChoix: ").strip()
        
        if choice == '1':
            print("\n" + "="*70)
            print("AJOUTER UNE SOURCE")
            print("="*70)
            print("\n💡 Sites recommandés pour tester:")
            print("  • http://books.toscrape.com/")
            print("  • https://news.ycombinator.com/")
            print("  • https://en.wikipedia.org/wiki/Web_scraping")
            print()
            
            url = input("URL: ").strip()
            source_type = input("Type [website]: ").strip() or 'website'
            frequency = input("Fréquence (hourly/daily/weekly/monthly) [daily]: ").strip() or 'daily'
            schedule_time = input("Heure (HH:MM) [09:00]: ").strip() or '09:00'
            max_hits = int(input("Max pages [100]: ").strip() or '100')
            content_types_input = input("Types (html,xml,rss,pdf,text) [html]: ").strip() or 'html'
            content_types = [ct.strip() for ct in content_types_input.split(',')]
            keywords_input = input("Mots-cles (finance, education, ... ) [vide]: ").strip()
            keywords = [kw.strip() for kw in keywords_input.split(',') if kw.strip()]
            
            source_id = crawler.add_source(
                url=url,
                source_type=source_type,
                frequency=frequency,
                schedule_time=schedule_time,
                max_hits=max_hits,
                content_types=content_types,
                keywords=keywords
            )
            print(f"\n✅ Source ajoutée! ID: {source_id}")
        
        elif choice == '2':
            sources = crawler.get_sources()
            print(f"\n" + "="*70)
            print(f"SOURCES ({len(sources)})")
            print("="*70)
            for i, source in enumerate(sources, 1):
                print(f"\n{i}. 🆔 {source['_id']}")
                print(f"   🌐 URL: {source['url']}")
                print(f"   📁 Type: {source['type']}")
                print(f"   ⏰ Fréquence: {source['frequency']}")
                print(f"   ✅ Actif: {'Oui' if source['enabled'] else 'Non'}")
                print(f"   📊 Statut: {source.get('status', 'N/A')}")
                print(f"   ❌ Échecs: {source.get('failed_attempts', 0)}")
                print(f"   ✔️  Succès: {source.get('success_count', 0)}")
                print(f"   🕐 Dernier crawl: {source.get('last_crawl', 'Jamais')}")
        
        elif choice == '3':
            source_id = input("\n🆔 ID de la source: ").strip()
            count = crawler.crawl_source(source_id)
            print(f"\n✅ {count} éléments collectés")
        
        elif choice == '4':
            sources = crawler.get_sources(enabled_only=True)
            print(f"\n🚀 Crawl de {len(sources)} sources...")
            total = 0
            for source in sources:
                count = crawler.crawl_source(source['_id'])
                total += count
            print(f"\n✅ Total: {total} éléments")
        
        elif choice == '5':
            query = input("\n🔎 Recherche: ").strip()
            results = crawler.search_data(query)
            print(f"\n" + "="*70)
            print(f"RÉSULTATS ({len(results)})")
            print("="*70)
            for i, result in enumerate(results[:10], 1):
                print(f"\n{i}. 📄 {result['title']}")
                print(f"   🌐 {result['url']}")
                print(f"   📁 Type: {result['content_type']}")
                if result.get('description'):
                    print(f"   📝 {result['description'][:150]}...")
        
        elif choice == '6':
            stats = crawler.get_statistics()
            print(f"\n" + "="*70)
            print("STATISTIQUES")
            print("="*70)
            print(f"📦 Total sources: {stats['total_sources']}")
            print(f"✅ Sources actives: {stats['active_sources']}")
            print(f"❌ Sources en échec: {stats['failed_sources']}")
            print(f"📄 Total données: {stats['total_data']}")
            print(f"🔗 URLs crawlées: {stats['urls_crawled']}")
        
        elif choice == '7':
            source_id = input("\n🗑️  ID à supprimer: ").strip()
            if crawler.delete_source(source_id):
                print("\n✅ Source supprimée")
            else:
                print("\n❌ Erreur")
        
        elif choice == '8':
            print("\n⏰ Démarrage du planificateur...")
            crawler.schedule_crawls()
            print("✅ Planificateur actif (Ctrl+C pour arrêter)")
            try:
                while True:
                    time.sleep(1)
            except KeyboardInterrupt:
                print("\n\n⏹️  Arrêté")
        
        elif choice == '9':
            crawler.close()
            print("\n👋 Au revoir!")
            break


if __name__ == "__main__":
    main()
