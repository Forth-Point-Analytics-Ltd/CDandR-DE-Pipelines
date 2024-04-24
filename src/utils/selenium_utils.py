import selenium.webdriver as webdriver
from selenium.webdriver.chrome.service import Service as ChromeService


def init_selenium():
    user_agent = """Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/89.0.4389.114 Safari/537.36"""
    service = ChromeService()
    options = webdriver.ChromeOptions()
    options.add_argument("--headless")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("start-maximized")
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    options.add_argument(f"user-agent={user_agent}")
    jsDriver = webdriver.Chrome(service=service, options=options)
    return jsDriver
