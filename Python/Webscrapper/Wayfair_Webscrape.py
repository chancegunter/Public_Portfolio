import os
import sys
import time
import glob
import shutil
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.common.exceptions import NoSuchElementException
from webdriver_manager.chrome import ChromeDriverManager
from webdriver_manager.core.driver_cache import DriverCacheManager
from selenium.webdriver.chrome.service import Service as ChromeService
from pathlib import Path

cur_dir = Path(__file__).parent
import keyring as kr


wayfair_cred = kr.get_credential("wayfair", None)

def initialize_driver():
    driver_path = 'local_path'
    cache_manager = DriverCacheManager(driver_path)
    driver = webdriver.Chrome(service=ChromeService(ChromeDriverManager(cache_manager=cache_manager).install()))
    return driver

def get_chromedriver_version(driver):
    try:
        # Get the version of Chrome WebDriver
        chromedriver_version = driver.capabilities['chrome']['chromedriverVersion'].split(' ')[0]
        print(f"Chrome WebDriver Version: {chromedriver_version}")
    except Exception as e:
        print("Error:", e)

def get_chrome_version(driver):
    try:
        # Get the version of Chrome installed on the system
        chrome_version = driver.capabilities['browserVersion']
        print(f"Chrome Version: {chrome_version}")
    except Exception as e:
        print("Error:", e)

def navigate_site(driver, username, password, original_path):
    options = webdriver.ChromeOptions()
    options.add_experimental_option('excludeSwitches', ['enable-logging'])
    options.add_argument('--log-level=3')
    driver.get('https://partners.wayfair.com/v/login/index')

    print('Logging in...')

    login_elements = driver.find_elements(By.CSS_SELECTOR, '#js-username, #username')
    password_elements = driver.find_elements(By.CSS_SELECTOR, '#password_field, #password')
    signin_elements = driver.find_elements(By.CSS_SELECTOR, '#login, #kc-login')
    
    if login_elements and password_elements and signin_elements:
        login_elements[0].send_keys(username)
        password_elements[0].send_keys(password)
        signin_elements[0].click()

    time.sleep(10)    

    inventory_url = 'https://partners.wayfair.com/v/wfs/products/product/index'
    driver.get(inventory_url)

    time.sleep(10) 
    process_region(driver, original_path, '/US.csv', 0)
    process_region(driver, original_path, '/CAN.csv', 1)
    time.sleep(10) 
    process_region(driver, original_path, '/EU.csv', 2)
    time.sleep(10) 

    return driver

def process_region(driver, original_path, file_name, button_index):
    print('Processing region...')
    time.sleep(3)
    switch_button = driver.find_element(By.CSS_SELECTOR, '[data-test-id="changeSupplierButton"]')
    switch_button.click()
    time.sleep(10)
    dropdown_items = driver.find_elements(By.CSS_SELECTOR,
                                          '.react-tiny-popover-container div[data-hb-id="homebase-box"] li')
    print(dropdown_items)
    time.sleep(10)
    region_button = dropdown_items[button_index]
    region_button.click()
    download_files(driver, original_path, file_name)



def download_files(driver, original_path, name):
    print('Downloading files...')
    time.sleep(4)
    for _ in range(4):
        try:
            export_button = driver.find_element(By.XPATH, '//button[contains(text(), "Export CSV")]')
            break
        except NoSuchElementException:
            time.sleep(2)
            print('Could not find export button')
    export_button.click()
    time.sleep(15)

    for _ in range(4):
        try:
            download_button = driver.find_element(By.XPATH, '//button[contains(text(), "Download")]')
            break
        except NoSuchElementException:
            time.sleep(15)
            print('Could not find download button')
            export_button = driver.find_element(By.XPATH,'//button[contains(text(), "Export CSV")]')
            export_button.click()
    download_button.click()
    time.sleep(50)

    for _ in range(4):
        try:
            dismiss_button = driver.find_element(By.CSS_SELECTOR, '[class="DismissButton__IconWrap-sc-1i1c6l4-0 eCUajQ"]')
            dismiss_button.click()

            button = driver.find_element(By.XPATH, '/html/body/div[2]/div/div/div/div/div[2]/div[1]/div/div/div/div[2]/div[1]/div/button/div[1]')
            time.sleep(15)
            driver.get('https://partners.wayfair.com/v/supplier/download_center/management/app')
            time.sleep(10)
            download_button = driver.find_element(By.XPATH, "/html/body/div[1]/div/div/div[2]/div[2]/div/div/div/div[3]/div[1]/div/div/div[2]/table/tbody/tr[1]/td[6]/span")
            download_button.click()
            driver.get('https://partners.wayfair.com/v/wfs/products/product/index')
            time.sleep(10)
            break
        except NoSuchElementException:
            time.sleep(10)

    move_files(original_path, name)

    # for _ in range(4):
    #     try:
    #         switch_button = driver.find_element(By.CSS_SELECTOR,'[data-test-id="changeSupplierButton"]')
    #         break
    #     except NoSuchElementException:
    #         print('Could not find switch button')
    # switch_button.click()
    # time.sleep(3)

def move_files(original_path, name):
    folder = str(Path.home() / 'Downloads')
    files = glob.glob(folder + '//*csv')
    top_file = max(files, key = os.path.getctime)
    os.rename(top_file, folder + name)
    file = folder + name
    os.chdir(original_path)
    os.chdir('local_path')
    folder = os.path.abspath(os.curdir)
    destination = folder + '/'
    shutil.move(file, destination)


# Usage example
def main(username, password, original_path):
    driver = initialize_driver()
    get_chromedriver_version(driver)
    get_chrome_version(driver)
    navigate_site(driver, username, password, original_path)

if __name__ == "__main__":
    original_path = os.path.abspath(os.curdir)
    username = wayfair_cred.username
    password = wayfair_cred.password
    main(username, password, original_path)

