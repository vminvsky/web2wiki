from selenium import webdriver
from PIL import Image

from pyvirtualdisplay import Display
from selenium.webdriver.chrome.service import Service

from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

import time
import pandas as pd

from io import BytesIO

from selenium.webdriver.common.by import By

from pyvirtualdisplay import Display

from tqdm import tqdm

def highlight(driver, element, effect_time, color, border):
    """Highlights (blinks) a Selenium Webdriver element"""
    def apply_style(s):
        driver.execute_script("arguments[0].setAttribute('style', arguments[1]);",
                              element, s)
    original_style = element.get_attribute('style')
    apply_style(f"border: {border}px solid {color};")
    time.sleep(effect_time)

class Screenshotter():
    def __init__(self, save_dir):
        display = Display(visible=0, size=(1920, 1080))  
        display.start()

        self.service = Service('/home/veselovs/chromedriver3')
        self.service.start()

        self.options = webdriver.ChromeOptions()
        self.driver = webdriver.Chrome('/home/veselovs/chrome_driver3', options = self.options)
        timeout = 25
        self.driver.set_page_load_timeout(timeout)
        
        self.save_dir = save_dir


    def extract_element(self, website_link, wiki):
        try:
            self.driver.get(website_link)
            time.sleep(3)
            element = self.driver.find_elements(By.XPATH, f'//a[contains(@href,"{wiki}")]')[0]
            parent = element.find_element(By.XPATH, "./..")
            self.driver.execute_script("arguments[0].scrollIntoView({block: 'center', inline: 'nearest'})", element)
            highlight(self.driver, element,5,"red",3)
            return element
        except:
            print("Didn't find element")
            parent = None
            return parent

    def take_screenshot(self,element, output_name):
        try:
            x = element.screenshot_as_png

            im = Image.open(BytesIO(x))
            im.save(self.save_dir + f'{output_name}.png')
#             self.driver.close()
            print("Saved image {}".format(output_name))
        except WebDriverException:
            print("Not wide enough")
            
        
    def iterate_over(self, url_links, wiki_links, output_names, element_screenshot = False):
        
        for url, wiki, output in tqdm(zip(url_links,wiki_links,output_names)):
            print(output)
            parent = self.extract_element(url, wiki)
            if parent != None:
                if element_screenshot == True:
                    self.take_screenshot(parent,output)
                else:
                    self.driver.save_screenshot(self.save_dir + f"{output}.png")
        self.driver.quit()