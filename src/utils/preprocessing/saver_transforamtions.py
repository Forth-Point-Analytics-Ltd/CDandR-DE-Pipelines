from decimal import Decimal
import re
from selenium.webdriver.common.by import By
from selenium.webdriver.remote.webelement import WebElement
from src.utils.types import PricePer, Quantity

def is_number(s):
    try:
        float(s)
        return True
    except ValueError:
        return False

def remove_element(driver, element):
    driver.execute_script(
        """
    var element = arguments[0];
    element.parentNode.removeChild(element);
    """,
        element,
    )


def remove_all_elements(
    driver, web_element, element_selector="span.item__price--theme-large"
):
    element_tags = web_element.find_elements(By.CSS_SELECTOR, element_selector)
    # Remove hidden tags
    for element_tag in element_tags:
        remove_element(driver, element_tag)


def get_item_info(item: WebElement, tag: str, css_class: str, attribute: str) -> str:
    return item.find_element(By.CSS_SELECTOR, value=f"{tag}.{css_class}").get_attribute(
        attribute
    )


def clean_price(driver, price_element: WebElement) -> Decimal:
    unit = get_item_info(price_element, "span", "item__price--theme-large", "innerHTML")
    remove_all_elements(
        driver, price_element, element_selector="span.item__price--theme-large"
    )
    decimals = price_element.get_attribute("innerHTML")
    price = f"{unit}{decimals}"
    find_pound = price.find("£")
    return Decimal(price[find_pound + 1 :].strip())


def clean_name(product_name: str) -> str:
    amp_string = "&amp;"
    return product_name.replace(amp_string, "&")


def parse_quantity(product_name: str) -> Quantity:
    # Regex to match patterns like '3x40', '3 x 10', '4x30ml', or '120ml'.
    # - (\d+)\s*x\s*(\d+)(\s?(ml|cl|g|kg|l))? captures multipliers (e.g., '3x40', '3 x 10') with optional units.
    # - (\d*\.?\d+)\s?(ml|cl|g|kg|l) captures standalone quantities with units (e.g., '120ml').

    pattern = re.compile(
        r"(\d*\.?\d+)\s?(ml|cl|g|kg|l|s|pk|litre|pack|pads|\'s)"
        r"|(\d*\.?\d+)\s?"
        r"(ml|cl|g|kg|l|s|pk|litre|pack|pads|\'s)",
        re.IGNORECASE,
    )
    match = pattern.search(product_name)
    if match:
        if match.group(1) and match.group(2) and match.group(3) and match.group(4):
            print("state 1")
            pass
        if match.group(1) and match.group(2) and match.group(3) and not match.group(4):
            print("state2")
            pass
        if match.group(1) and match.group(2) and not match.group(3) and not match.group(4):
            if is_number(match.group(1)) and (not is_number(match.group(2))):
                multiplier= 1
                value = match.group(1)
                unit = match.group(2)
    else:
        # Defaults if no matching pattern is found
        multiplier = 1
        value = 1
        unit = "ea"

    match unit.lower():
        case "s" | "S" | "'s" | "pk" | "pack" | "pads":
            new_unit = "ea"
        case "litre" | "l":
            value = Decimal(value) * 1000
            new_unit = "ml"
        case "cl":
            value = Decimal(value) * 10
            new_unit = "ml"
        case _:
            new_unit = unit.lower()
    if type(value) == str:
        if is_number(value):
            value = int(value)
        else:
            new_unit = value
            value = 1
    return Quantity(int(multiplier), value, new_unit)


def calculate_price_per(price: Decimal, quantity: Quantity) -> PricePer:
    match quantity.unit:
        case "g":
            price_per_unit = "100g"
            mult = 100
        case "ml":
            price_per_unit = "100ml"
            mult = 100
        case "ea":
            price_per_unit = "ea"
            mult = 1
        case _:
            price_per_unit = None
            mult = 1
    total_mult = Decimal(mult / quantity.multiplier)
    price_per = Decimal(price / quantity.quantity * total_mult)
    return PricePer(price_per, price_per_unit)