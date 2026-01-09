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
        # Utilisation de valeurs par défaut pour éviter les plantages si Variables absentes
        self.remote_url = Variable.get("SELENIUM_REMOTE_URL")
        self.wait_time = int(Variable.get("SELENIUM_WAIT_TIME", default_var=7))
        self.driver = self._create_driver()

    def _create_driver(self):
        options = Options()
        
        # 1. Arguments de performance (Headless et réduction de charge)
        arguments = [
            "--headless=new", 
            "--disable-gpu", 
            "--no-sandbox",
            "--disable-dev-shm-usage", 
            "--disable-extensions",
            "--blink-settings=imagesEnabled=false", # BLOQUE LE CHARGEMENT DES IMAGES
            "--disable-notifications",
            "--disable-popup-blocking",
            "--lang=en-US",
            "log-level=3" # Réduit le bruit dans les logs
        ]
        
        for arg in arguments:
            options.add_argument(arg)

        # 2. Préférences pour bloquer davantage de contenus (Images, CSS, Flash)
        prefs = {
            "profile.managed_default_content_settings.images": 2, # Bloque les images
            "profile.default_content_setting_values.notifications": 2,
            "profile.managed_default_content_settings.stylesheets": 2, # Optionnel : bloque le CSS
            "profile.managed_default_content_settings.cookies": 2, # Optionnel : peut booster si FlightAware l'accepte
        }
        options.add_experimental_option("prefs", prefs)
        
        driver = webdriver.Remote(command_executor=self.remote_url, options=options)
        
        # 3. Timeout de chargement de page (Indispensable pour tenir les 5 min)
        # Si la page prend plus de 15s à charger, on passe au vol suivant
        driver.set_page_load_timeout(15) 
        
        logging.info("Selenium driver created (Images disabled, lightweight mode)")
        return driver

    def request(self, selector, timeout=None, retries=2): # Réduit retries de 3 à 2 pour gagner du temps
        timeout = timeout or self.wait_time
        
        for attempt in range(retries):
            try:
                # Utilisation de presence_of_element_located pour plus de rapidité (pas besoin de visibilité)
                element = WebDriverWait(self.driver, timeout).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, selector))
                )
                return element.text.strip().replace("\xa0", " ")
            except TimeoutException:
                if attempt < retries - 1:
                    logging.warning(f"Timeout on {selector}, retry {attempt + 1}")
                    # On évite le time.sleep(backoff) pour ne pas rallonger le DAG
                    continue
                break
            except Exception as e:
                logging.error(f"Selenium request error: {e}")
                break
        return None

    def close(self):
        try:
            self.driver.quit()
            logging.info("Selenium driver closed")
        except Exception:
            pass