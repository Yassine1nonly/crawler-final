import os
from typing import Optional, Tuple


class BrowserFetcher:
    def fetch_with_playwright(self, url: str, timeout_ms: int) -> Optional[Tuple[str, str, str]]:
        try:
            base_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
            browsers_path = os.path.join(base_dir, ".playwright")
            os.makedirs(browsers_path, exist_ok=True)
            os.environ.setdefault("PLAYWRIGHT_BROWSERS_PATH", browsers_path)
            from playwright.sync_api import sync_playwright
        except Exception:
            return None

        try:
            with sync_playwright() as p:
                browser = p.chromium.launch(headless=True, timeout=timeout_ms)
                page = browser.new_page()
                page.set_default_timeout(timeout_ms)
                page.goto(url, timeout=timeout_ms, wait_until="domcontentloaded")
                content = page.content()
                final_url = page.url
                browser.close()
                return content, final_url, "playwright"
        except Exception:
            return None

    def fetch_with_selenium(self, url: str, timeout_sec: int) -> Optional[Tuple[str, str, str]]:
        try:
            base_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
            cache_path = os.path.join(base_dir, ".selenium")
            os.makedirs(cache_path, exist_ok=True)
            os.environ.setdefault("SELENIUM_MANAGER_CACHE", cache_path)
            from selenium import webdriver
            from selenium.webdriver.chrome.options import Options
        except Exception:
            return None

        driver = None
        try:
            options = Options()
            options.add_argument("--headless=new")
            options.add_argument("--disable-gpu")
            options.add_argument("--no-sandbox")
            options.add_argument("--disable-dev-shm-usage")
            driver = webdriver.Chrome(options=options)
            driver.set_page_load_timeout(timeout_sec)
            driver.get(url)
            content = driver.page_source
            final_url = driver.current_url
            return content, final_url, "selenium"
        except Exception:
            return None
        finally:
            if driver is not None:
                try:
                    driver.quit()
                except Exception:
                    pass

    def fetch(self, url: str, timeout_sec: int) -> Optional[Tuple[str, str, str]]:
        timeout_ms = int(timeout_sec * 1000)
        result = self.fetch_with_playwright(url, timeout_ms)
        if result:
            return result
        return self.fetch_with_selenium(url, timeout_sec)
