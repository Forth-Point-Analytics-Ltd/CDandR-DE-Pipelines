from typing import Optional
from src.transformations.to_landing.base_web_scraper import BaseWebScraper


class SaversWebScraper(BaseWebScraper):
    def __init__(
        url: str,
        destination_path: str,
        brands_product_config: str,
        url_leaf_config: Optional[str] = None,
    ):
        pass
