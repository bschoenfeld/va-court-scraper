from playwright.sync_api import sync_playwright

_playwright_instance = None

def get_playwright():
    global _playwright_instance
    if _playwright_instance is None:
        _playwright_instance = sync_playwright().start()
    return _playwright_instance
