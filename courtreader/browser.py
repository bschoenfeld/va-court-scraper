from playwright.sync_api import sync_playwright

def get_playwright():
    return sync_playwright().start()
