import bootstrap_init
from selenium.webdriver.chrome.options import Options

def setChromeOptions(global_instance):
    """
    Doc String
    """
    logger = bootstrap_init.logger

    chrome_options = Options()
    chrome_options.add_argument("--headless=new") # for Chrome >= 109
    prefs = {
    "profile.managed_default_content_settings.images": 2,  # Disable images
    }
    chrome_options.add_experimental_option("prefs", prefs)
    global_instance.modify_data("chrome_options", chrome_options)
    logger.success(f"Chrome Options saved in Global Instance!")
    return