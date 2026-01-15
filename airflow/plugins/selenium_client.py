import logging

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait

from airflow.models import Variable

class SeleniumClient:
	def __init__(self):
		# Récupération des variables Airflow
		self.remote_url = Variable.get("SELENIUM_REMOTE_URL")
		self.wait_time = int(Variable.get("SELENIUM_WAIT_TIME"))

		self.driver = self._create_driver()

	def _create_driver(self):
		options = Options()
		
		# 1. Arguments de performance pure
		arguments = [
			"--headless=new", 
			"--disable-gpu", 
			"--no-sandbox",
			"--disable-extensions",
			"--blink-settings=imagesEnabled=false",
			"--lang=en-US",
			"log-level=3",
			"--disable-blink-features=AutomationControlled"
		]
		for arg in arguments:
			options.add_argument(arg)

		# 2. Préférences pour bloquer le chargement
		prefs = {
			"profile.managed_default_content_settings.images": 2,
			"profile.managed_default_content_settings.stylesheets": 2,
			"profile.default_content_setting_values.cookies": 2,
		}
		options.add_experimental_option("prefs", prefs)
		
		driver = webdriver.Remote(command_executor=self.remote_url, options=options)
		
		# 3. Timeout de chargement de page
		driver.set_page_load_timeout(10) 
		
		logging.info("Selenium driver created (Ultra-Lightweight Mode)")
		return driver

	def request(self, selector, timeout=None):
		try:
			element = WebDriverWait(self.driver, timeout or self.wait_time).until(
				EC.presence_of_element_located((By.CSS_SELECTOR, selector))
			)

			return element.text.strip().replace("\xa0", " ")
		
		except Exception:
			return None

	def close(self):
		"""Libère les ressources immédiatement."""
		try:
			if self.driver:
				self.driver.quit()
				logging.info("Selenium driver closed and resources freed")
		except Exception as e:
			logging.debug(f"Error during driver quit: {e}")