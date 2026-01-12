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
            'Accept-Encoding': 'gzip, deflate, br',
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
            # Délai plus long en cas de retry
            return base_delay * 2 + random.uniform(2, 5)
        
        # Variation naturelle humaine
        human_variance = random.uniform(-0.5, 1.5)
        
        # Ajout de patterns humains (parfois très rapide, parfois lent)
        if random.random() < 0.1:  # 10% du temps, très rapide
            return base_delay * 0.5 + human_variance
        elif random.random() < 0.15:  # 15% du temps, lent
            return base_delay * 2 + human_variance
        
        return base_delay + human_variance
    
    def create_advanced_session(self, use_proxy=False, verify_ssl=True):
        """Crée une session avec configuration avancée"""
        session = requests.Session()
        
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
        
        # Enlever le fragment (#)
        url_without_fragment = url.split('#')[0]
        
        # Trier les paramètres de query pour cohérence
        if parsed.query:
            params = parse_qs(parsed.query)
            sorted_params = sorted(params.items())
            normalized_query = urlencode(sorted_params, doseq=True)
            normalized = f"{parsed.scheme}://{parsed.netloc}{parsed.path}?{normalized_query}"
        else:
            normalized = url_without_fragment
        
        # Enlever le trailing slash sauf pour la racine
        if normalized.endswith('/') and parsed.path != '/':
            normalized = normalized[:-1]
        
        return normalized


class AdaptiveRateLimiter:
    """Rate limiter adaptatif qui apprend des réponses du serveur"""
    
    def __init__(self):
        self.domain_timers = {}
        self.domain_delays = defaultdict(lambda: 2.0)  # Délai initial 2s
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
            if self.domain_delays[domain] > 2.0:
                self.domain_delays[domain] = max(
                    self.domain_delays[domain] * 0.95,
                    2.0  # Min 2 secondes
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
                 base_delay=2,
                 respect_robots_txt=True,
                 verify_ssl=True,
                 max_retries_per_url=3):
        """Initialise le crawler"""
        try:
            self.client = pymongo.MongoClient(mongo_uri)
            self.db = self.client[db_name]
            self.sources_collection = self.db['sources']
            self.data_collection = self.db['crawled_data']
            self.robots_cache = self.db['robots_cache']
            self.url_history = self.db['url_history']
            
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
            
            # Stratégies anti-blocage
            self.rate_limiter = AdaptiveRateLimiter()
            self.anti_blocking = AdvancedAntiBlockingStrategy()
            self.js_solver = JavaScriptChallengeSolver()
            
            logger.info(f"✓ MongoDB: {db_name}")
            logger.info(f"✓ Config: proxy={use_proxy}, delay={base_delay}s, SSL={verify_ssl}")
            logger.info(f"✓ Stratégies avancées activées")
        except Exception as e:
            logger.error(f"Erreur MongoDB: {e}")
            raise
    
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
        recent = self.url_history.find_one({
            'url': url,
            'last_crawled': {'$gte': datetime.now() - timedelta(hours=hours)}
        })
        return recent is not None
    
    def mark_url_crawled(self, url, success=True):
        """Marque une URL comme crawlée"""
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
                   max_hits=100, content_types=None,
                   enabled=True):
        """Ajoute une source"""
        if content_types is None:
            content_types = ['html', 'text']
        
        source = {
            'url': url,
            'type': source_type,
            'frequency': frequency,
            'schedule_time': schedule_time,
            'max_hits': max_hits,
            'content_types': content_types,
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
    
    def crawl_url(self, url, content_types, max_hits=100):
        """Crawl avec stratégies anti-blocage avancées"""
        collected_data = []
        visited_urls = set()
        urls_to_visit = [url]
        failed_urls = {}  # URL -> (retry_count, last_error)
        
        session = self.anti_blocking.create_advanced_session(
            use_proxy=self.use_proxy,
            verify_ssl=self.verify_ssl
        )
        
        domain = urlparse(url).netloc
        last_referer = None
        
        while urls_to_visit and len(collected_data) < max_hits:
            current_url = urls_to_visit.pop(0)
            normalized_url = self.anti_blocking.normalize_url(current_url)
            
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
                continue
            
            # Éviter de re-crawler trop vite
            if self.is_url_recently_crawled(normalized_url, hours=1):
                logger.debug(f"Déjà crawlé récemment: {current_url}")
                continue
            
            visited_urls.add(normalized_url)
            
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
                    timeout=30,
                    allow_redirects=True
                )
                
                # Gestion des codes d'erreur
                if response.status_code == 429:
                    logger.warning(f"⏱️  429 Rate Limited: {current_url}")
                    self.rate_limiter.report_429(domain)
                    retry_after = int(response.headers.get('Retry-After', 60))
                    logger.info(f"Attente de {retry_after}s...")
                    time.sleep(retry_after)
                    urls_to_visit.insert(0, current_url)
                    visited_urls.remove(normalized_url)
                    continue
                
                if response.status_code in [401, 403]:
                    logger.warning(f"🚫 {response.status_code} Accès refusé: {current_url}")
                    
                    # Détecter challenge JavaScript
                    if self.js_solver.detect_challenge(response):
                        logger.warning("⚠️  Protection anti-bot détectée!")
                        for msg in self.js_solver.suggest_solutions():
                            logger.info(msg)
                    
                    failed_urls[normalized_url] = (
                        failed_urls.get(normalized_url, (0, ""))[0] + 1,
                        f"HTTP {response.status_code}"
                    )
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
                
                if 'html' in content_type and 'html' in content_types:
                    data = self._process_html(current_url, response.content)
                    if data:
                        collected_data.append(data)
                        logger.info(f"✅ Collecté: {data['title'][:60]}")
                        
                        # Extraire liens si besoin
                        if len(collected_data) < max_hits:
                            soup = BeautifulSoup(response.content, 'html.parser')
                            links_found = 0
                            for link in soup.find_all('a', href=True):
                                absolute_url = urljoin(current_url, link['href'])
                                clean_url = self.anti_blocking.normalize_url(absolute_url)
                                
                                if self._is_same_domain(url, clean_url):
                                    if clean_url not in visited_urls and clean_url not in [f[0] for f in failed_urls]:
                                        if clean_url not in urls_to_visit:
                                            urls_to_visit.append(clean_url)
                                            links_found += 1
                            
                            if links_found > 0:
                                logger.info(f"   → {links_found} nouveaux liens")
                        
                        last_referer = current_url
                
                elif 'xml' in content_type and 'xml' in content_types:
                    data = self._process_xml(current_url, response.content)
                    if data:
                        collected_data.append(data)
                        logger.info(f"✅ XML collecté: {data['title'][:60]}")
                
                elif 'pdf' in content_type and 'pdf' in content_types:
                    data = self._process_pdf(current_url, response.content)
                    if data:
                        collected_data.append(data)
                        logger.info(f"✅ PDF collecté: {data['title'][:60]}")
                
                elif 'text' in content_type and 'text' in content_types:
                    data = self._process_text(current_url, response.text)
                    if data:
                        collected_data.append(data)
                        logger.info(f"✅ Texte collecté: {data['title'][:60]}")
                
                else:
                    # Essayer HTML par défaut
                    if 'html' in content_types:
                        data = self._process_html(current_url, response.content)
                        if data:
                            collected_data.append(data)
                            logger.info(f"✅ Page collectée: {data['title'][:60]}")
                
                if data:
                    self.mark_url_crawled(normalized_url, success=True)
                
            except requests.exceptions.Timeout:
                logger.warning(f"⏱️  Timeout: {current_url}")
                failed_urls[normalized_url] = (
                    failed_urls.get(normalized_url, (0, ""))[0] + 1,
                    "Timeout"
                )
                
            except requests.exceptions.ConnectionError as e:
                logger.warning(f"🔌 Erreur connexion: {current_url}")
                failed_urls[normalized_url] = (
                    failed_urls.get(normalized_url, (0, ""))[0] + 1,
                    "Connection Error"
                )
                time.sleep(5)
                
            except requests.exceptions.TooManyRedirects:
                logger.warning(f"🔄 Trop de redirections: {current_url}")
                failed_urls[normalized_url] = (999, "Too Many Redirects")
                
            except Exception as e:
                logger.warning(f"❌ Erreur: {current_url} - {str(e)[:100]}")
                failed_urls[normalized_url] = (
                    failed_urls.get(normalized_url, (0, ""))[0] + 1,
                    str(e)[:100]
                )
        
        session.close()
        logger.info(f"📊 Résumé: {len(collected_data)} pages collectées, {len(failed_urls)} échecs")
        
        return collected_data
    
    def _is_same_domain(self, base_url, check_url):
        """Vérifie si même domaine"""
        return urlparse(base_url).netloc == urlparse(check_url).netloc
    
    def _process_html(self, url, content):
        """Traite HTML"""
        try:
            soup = BeautifulSoup(content, 'html.parser')
            
            for script in soup(['script', 'style', 'nav', 'footer', 'aside']):
                script.decompose()
            
            title = soup.title.string if soup.title else 'Sans titre'
            title = title.strip()[:200]
            
            text_content = soup.get_text(separator=' ', strip=True)
            
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
                source['max_hits']
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
            content_types_input = input("Types (html,xml,pdf,text) [html]: ").strip() or 'html'
            content_types = [ct.strip() for ct in content_types_input.split(',')]
            
            source_id = crawler.add_source(
                url=url,
                source_type=source_type,
                frequency=frequency,
                schedule_time=schedule_time,
                max_hits=max_hits,
                content_types=content_types
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