from typing import List, Tuple
from datetime import datetime
import pandas as pd
from selenium.common.exceptions import (
    TimeoutException,
    WebDriverException,
)
from selenium.webdriver.common.by import By
from selenium.webdriver.remote.webelement import WebElement
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait

import urllib.parse

from src.utils.file_handler import load_json
from src.utils.preprocessing.saver_transforamtions import (
    calculate_price_per,
    clean_name,
    clean_price,
    get_item_info,
    parse_quantity,
)
from src.utils.selenium_utils import init_selenium
from src.utils.spark_utils import create_abfss_path
from src.utils.types import ProductInfo


class SaversWebScraper:
    def __init__(
        self, url: str, brand_data_file_path: str, dst_path: str
    ) -> None:
        self.driver = init_selenium()
        self.wait = WebDriverWait(self.driver, 10)
        brand_data = load_json(brand_data_file_path)
        (
            self.cdr_brand_urls,
            self.competitor_brand_urls,
        ) = self.split_into_brand_competitor_urls(url, brand_data)
        self.dst_path = dst_path

    def compose_savers_urls(self, base_url: str, brand: str) -> str:
        return f"{base_url}/b/{urllib.parse.quote(brand)}"

    def split_into_brand_competitor_urls(self, url, brand_data):
        cdr_brand_urls = []
        competitor_brand_urls = []
        for cdr_brand, competitor_brands in brand_data.items():
            cdr_brand_url = self.compose_savers_urls(url, cdr_brand)
            cdr_brand_urls.append((cdr_brand, cdr_brand_url))
            for competitor_brand in competitor_brands:
                competitor_brand_url = self.compose_savers_urls(
                    url, competitor_brand
                )
                competitor_brand_urls.append(
                    (competitor_brand, competitor_brand_url)
                )

        return cdr_brand_urls, competitor_brand_urls

    def load_and_find(self, element: str) -> WebElement:
        try:
            self.wait = WebDriverWait(self.driver, 10)
            self.wait.until(
                EC.visibility_of_all_elements_located(
                    (By.CSS_SELECTOR, element)
                )
            )
        except TimeoutException:
            raise

        # Find all product items
        web_element = self.driver.find_elements(
            By.CSS_SELECTOR,
            element,
        )
        return web_element

    def get_product_space(self):
        product_space = None
        try:
            product_space = self.load_and_find(
                "div.plp",
            )
        except TimeoutException:
            raise
        except Exception:
            raise
        return product_space

    def get_product_items(self, product_space: WebElement):
        return product_space.find_elements(By.CSS_SELECTOR, "li.item__gutter")

    def get_product_info(self, product: WebElement, brand: str) -> ProductInfo:
        product_name = clean_name(
            get_item_info(product, "a", "item__productName", "innerHTML")
        )
        product_price_now = clean_price(
            self.driver,
            product.find_element(By.CSS_SELECTOR, "span.item__price"),
        )
        product_url = get_item_info(product, "a", "item__productName", "href")

        quantity = parse_quantity(product_name)
        product_quantity = quantity.multiplier
        product_size = quantity.quantity
        product_size_unit = quantity.unit
        price_per = calculate_price_per(product_price_now, quantity)
        timestamp = datetime.utcnow()
        brand_name = brand
        product_price_per = price_per.price
        product_price_per_unit = price_per.unit

        return ProductInfo(
            timestamp,
            product_name,
            brand_name,
            product_url,
            product_price_now,
            product_quantity,
            product_size,
            product_size_unit,
            product_price_per,
            product_price_per_unit,
        )

    def get_shelf_products(self, product_items: List[WebElement], brand):
        shelf_products = []
        for item in product_items:
            shelf_products.append(self.get_product_info(item, brand))
        return shelf_products

    def process_brand_urls(self, brand_urls: List[Tuple[str, str]]):
        for brand, brand_url in brand_urls:
            self.driver.get(brand_url)
            stop = False
            try:
                while not stop:
                    product_space = self.load_and_find("div.plp")
                    if len(product_space) != 0:
                        product_items = product_space[0].find_elements(
                            By.CSS_SELECTOR, "li.item__gutter"
                        )
                        shelf_products = self.get_shelf_products(
                            product_items, brand
                        )
                        next_page_link = product_space[0].find_elements(
                            By.CSS_SELECTOR, "a.icon-angle-right"
                        )
                        if len(next_page_link) != 0:
                            next_page_link = next_page_link[0].get_attribute(
                                "href"
                            )
                            print(next_page_link)
                            self.driver.get(next_page_link)
                        else:
                            stop = True
                    else:
                        stop = True
            except (TimeoutException, WebDriverException) as e:
                print("Page does not exist")

    def run(self):
        products = []
        products.extend(self.process_brand_urls(self.cdr_brand_urls))
        products.extend(self.process_brand_urls(self.competitor_brand_urls))
        pd.DataFrame(products).to_csv(self.dst_path, index=False)
