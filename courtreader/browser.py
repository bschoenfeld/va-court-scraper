from playwright.sync_api import sync_playwright
import subprocess

def get_playwright():
    mgr = sync_playwright()
    return mgr, mgr.start()

def kill_playwright_processes():
    try:
        subprocess.call(['taskkill', '/F', '/T', '/IM', 'node.exe'], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        subprocess.call(['taskkill', '/F', '/T', '/IM', 'chrome.exe'], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        subprocess.call(['taskkill', '/F', '/T', '/IM', 'chromium.exe'], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        subprocess.call(['taskkill', '/F', '/T', '/IM', 'firefox.exe'], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        subprocess.call(['taskkill', '/F', '/T', '/IM', 'msedge.exe'], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    except Exception:
        pass
