import logging
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from airflow.models import Variable

class SeleniumClient:
	def __init__(self):
		# Récupération des variables Airflow
		self.remote_url = Variable.get("SELENIUM_REMOTE_URL")
		# On descend à 4 secondes par défaut pour le scraping rapide
		self.wait_time = int(Variable.get("SELENIUM_WAIT_TIME", default_var=4))
		self.driver = self._create_driver()

	def _create_driver(self):
		options = Options()
		
		# 1. Arguments de performance pure
		arguments = [
			"--headless=new", 
			"--disable-gpu", 
			"--no-sandbox",
			"--disable-extensions",
			"--blink-settings=imagesEnabled=false", # Désactive le moteur de rendu des images
			"--lang=en-US",
			"log-level=3",
			"--disable-blink-features=AutomationControlled"
		]
		for arg in arguments:
			options.add_argument(arg)

		# 2. Préférences pour bloquer le chargement (Images et Stylesheets)
		# C'est ce qui fait gagner le plus de temps sur FlightAware
		prefs = {
			"profile.managed_default_content_settings.images": 2,
			"profile.managed_default_content_settings.stylesheets": 2, # Bloque le CSS
			"profile.default_content_setting_values.cookies": 2, # Optionnel : à tester si FlightAware bloque
		}
		options.add_experimental_option("prefs", prefs)
		
		driver = webdriver.Remote(command_executor=self.remote_url, options=options)
		
		# 3. Timeout de chargement de page ultra-agressif
		# Si la page ne répond pas en 10s, on passe à la suite pour ne pas bloquer le DAG
		driver.set_page_load_timeout(10) 
		
		logging.info("Selenium driver created (Ultra-Lightweight Mode)")
		return driver

	def request(self, selector, timeout=None):
		"""
		Version ultra-rapide : On ne cherche que la présence dans le DOM.
		Pas de retry interne ici, c'est le DAG qui s'en occupe au prochain tour.
		"""
		try:
			# presence_of_element_located est beaucoup plus rapide que visibility
			element = WebDriverWait(self.driver, timeout or self.wait_time).until(
				EC.presence_of_element_located((By.CSS_SELECTOR, selector))
			)
			# On récupère le texte brut sans attendre de mise en forme
			return element.text.strip().replace("\xa0", " ")
		except Exception:
			# On reste silencieux pour la performance, on renvoie juste None
			return None

	def close(self):
		"""Libère les ressources immédiatement."""
		try:
			if self.driver:
				self.driver.quit() # quit() ferme tout (processus et fenêtres)
				logging.info("Selenium driver closed and resources freed")
		except Exception as e:
			logging.debug(f"Error during driver quit: {e}")