import logging
import time

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait

from airflow.models import Variable

from prometheus_client import CollectorRegistry, Counter, Histogram, push_to_gateway

class SeleniumClient:
	def __init__(self):
		# Récupération des variables Airflow
		self.remote_url = Variable.get("SELENIUM_REMOTE_URL")
		self.wait_time = int(Variable.get("SELENIUM_WAIT_TIME"))
		self.pushgateway_url = Variable.get("PUSHGATEWAY_URL")
		
		# --- Initialisation Prometheus ---
		self.registry = CollectorRegistry()
		
		# Métriques
		self.metric_request_duration = Histogram(
			'selenium_request_duration_seconds', 
			'Temps de recherche d un élément CSS',
			buckets=[0.5, 1.0, 2.0, 4.0, 10.0],
			registry=self.registry
		)
		self.metric_request_total = Counter(
			'selenium_requests_total', 
			'Total des requêtes Selenium par statut',
			['status'], # 'success' ou 'timeout'
			registry=self.registry
		)

		self.driver = self._create_driver()

	def _push_metrics(self):
		"""Envoie les métriques au Pushgateway."""
		try:
			push_to_gateway(self.pushgateway_url, job='airflow_selenium', registry=self.registry)
		except Exception as e:
			logging.warning(f"Prometheus push failed for Selenium: {e}")

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
		start_time = time.time()
		try:
			element = WebDriverWait(self.driver, timeout or self.wait_time).until(
				EC.presence_of_element_located((By.CSS_SELECTOR, selector))
			)
			
			# Calcul de la durée et log succès
			duration = time.time() - start_time
			self.metric_request_duration.observe(duration)
			self.metric_request_total.labels(status='success').inc()
			
			return element.text.strip().replace("\xa0", " ")
		
		except Exception:
			# Log échec
			self.metric_request_total.labels(status='timeout').inc()
			return None
		finally:
			# On pousse les métriques après chaque requête pour un monitoring fin
			self._push_metrics()

	def close(self):
		"""Libère les ressources immédiatement."""
		try:
			if self.driver:
				self.driver.quit()
				logging.info("Selenium driver closed and resources freed")
				# On s'assure que les dernières métriques sont envoyées avant de fermer
				self._push_metrics()
		except Exception as e:
			logging.debug(f"Error during driver quit: {e}")