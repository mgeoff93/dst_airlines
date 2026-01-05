import logging
import time
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException
from airflow.models import Variable

class SeleniumClient:
    def __init__(self):
        self.remote_url = Variable.get("SELENIUM_REMOTE_URL")
        self.wait_time = int(Variable.get("SELENIUM_WAIT_TIME"))
        self.driver = self._create_driver()

    def _create_driver(self):
        options = Options()
        arguments = [
            "--headless=new", "--disable-gpu", "--no-sandbox",
            "--disable-dev-shm-usage", "--disable-background-networking",
            "--disable-sync", "--no-first-run", "--lang=en-US"
        ]
        for arg in arguments:
            options.add_argument(arg)
        
        driver = webdriver.Remote(command_executor=self.remote_url, options=options)
        logging.info("Selenium driver created")
        return driver

    def request(self, selector, timeout=None, retries=3, backoff=1.5):
        timeout = timeout or self.wait_time
        
        for attempt in range(retries):
            try:
                element = WebDriverWait(self.driver, timeout).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, selector))
                )
                return element.text.strip().replace("\xa0", " ")
            except TimeoutException:
                if attempt < retries - 1:
                    wait = backoff ** attempt
                    time.sleep(wait)
                continue
            except Exception:
                break
        return None

    def close(self):
        try:
            self.driver.quit()
            logging.info("Selenium driver closed")
        except Exception:
            pass