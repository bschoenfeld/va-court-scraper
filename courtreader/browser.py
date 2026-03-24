from playwright.sync_api import sync_playwright

def get_playwright():
    mgr = sync_playwright()
    return mgr, mgr.start()
