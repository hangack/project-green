# /e/Fear/Univ/Big_data/Training/python/python-crawling/venv/Scripts/python
# -*- encoding: utf-8 -*-

import pandas as pd
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import time
import re
import datetime

import crawl_acc_sql as accsql


pd.set_option("display.max_rows", 200)
pd.set_option("display.max_columns", 200)
pd.set_option("display.width", 100)

options = webdriver.ChromeOptions()
#options.add_argument("headless")
options.add_argument("window-size=1100,850")
driver = webdriver.Chrome(executable_path="E:/Fear/Univ/Big_data/Training/python/python-crawling/chromedriver.exe", options=options)
#driver = webdriver.Firefox(executable_path="E:/Fear/Univ/Big_data/Training/python/python-crawling/geckodriver.exe")

df = pd.DataFrame(columns=["date", "seller", "server", "currency", "price", "stock"])

def get_page_set():
    #url = "https://www.g2g.com/categories/new-world-coins"
    url = "https://www.g2g.com/categories/path-of-exile-global-currency?fa=lgc_19398_tier%3Algc_19398_tier_42693,lgc_19398_tier_42698"
    class_name = "q-pa-md.q-pb-lg"

    soup = get_soup(url,class_name)

    a_list = soup.find_all("a", class_="full-height column rounded-borders cursor-pointer g-card-no-deco g-card-hover g-border-light")

    href_set = set()
    for a in a_list:
        href_set.add(a.get("href"))


    i = 1
    if bool(soup.find("div", class_="row justify-center q-mt-xl q-mb-lg")):
        class_name = "q-btn.q-btn-item.non-selectable.no-outline.q-btn--flat.q-btn--rectangle.text-dark.q-btn--actionable.q-focusable.q-hoverable.q-btn--wrap"
        while True:
            print(len(href_set))
            print("page"+str(i))
            i += 1
            url_page = url+"?page="+str(i)
            soup = get_soup(url_page,class_name)
            if soup.find("a", class_="full-height column rounded-borders cursor-pointer g-card-no-deco g-card-hover g-border-light") is None:
                print("exit")
                break

            a_list = soup.find_all("a", class_="full-height column rounded-borders cursor-pointer g-card-no-deco g-card-hover g-border-light")

            for href in a_list:
                href_set.add(href.get("href"))

    else:
        print("URL has single page")

    print(href_set)
    return href_set

def open_links(href_set):
    for link in href_set:
        driver.execute_script("window.open('"+link+"');")

def get_data():

    def get_data_seller(soup,selector):
        seller_list = soup.select(selector)
        seller_list_text = []
        for seller in seller_list:
            seller_list_text.append(seller.string)
        return seller_list_text

    def get_data_stock(soup):
        option_list = soup.select("div.offers-bottom-attributes.offer__content-lower-items > span")
        stock_list = []
        n = 0
        for option in option_list:
            n += 1
            if n%3 != 2:
                continue
            stock_list.append(option.string)
        return stock_list

    def try_clickable(xpath_name):
        try:
            i = 0
            soup = soup_wait_clickable(xpath_name)

            seller.extend(get_data_seller(soup,
                "a.flex.prechekout-non-produdct-details > div.seller-details.m-l-sm > div.seller__name-detail"))
            price.extend(get_data_seller(soup,
                                         "div.hide > div > div > div > div > div > span.offer-price-amount"))
            stock.extend(get_data_stock(soup))
        except IndexError as e:
            i += 1
            print(e+str(i))
            time.sleep(1)
            soup = try_clickable()
        return soup


    driver.switch_to.window(driver.window_handles[-1])
    class_name = "offers-bottom-attributes.offer__content-lower-items"
    soup = soup_wait(class_name)

    if (soup.find("div", class_="noresult-main-title").string == "The offer you try to view is no longer available."):
        driver.close()
        return False

    today = datetime.date.today().strftime("%Y-%m-%d")

    region_right_tolist = soup.select("div.region_right-detail")
    #print(region_right_tolist)
    data_col = []
    for list in region_right_tolist:
        data_col.append(list.string)
    #print(data_col)
    server = data_col[0][49:-44]
    currency = data_col[1][49:-44]

    seller = []
    price = []
    stock = []

    seller.extend(get_data_seller(soup,
                                  "a.flex.prechekout-non-produdct-details > div.seller-details.m-l-sm > div.seller__name-detail"))
    price.extend(get_data_seller(soup,
                                 "div.hide > div > div > div > div > div > span.offer-price-amount"))
    stock.extend(get_data_stock(soup))


    i = 1
    xpath_name = '//a[@href="#!-1"]'
    if bool(soup.find("a", class_="cdp_i")):
        while True:
            i += 1
            print("page"+str(i))
            ## 스크롤 내리기
            driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            ## Next 클릭
            driver.find_element_by_xpath('//a[@href="#!+1"]').click()

            soup = try_clickable(xpath_name)

            cdp_i = soup.find_all("a", class_="cdp_i")
            cdp = soup.find("div", class_="content_detail__pagination cdp")
            cdp = str(cdp)
            actpage = int(re.search('actpage="(.+?)" class', cdp).group(1))

            if actpage == (len(cdp_i) - 2):
                print("exit")
                break


    for i in range(len(seller)):
        data = [today,seller[i],server,currency,price[i],stock[i]]
        df.loc[len(df)] = data

    driver.close()


def get_soup(URL,by_name):
    driver.get(URL)
    return soup_wait(by_name)

def soup_wait(by_name):
    wait = WebDriverWait(driver, 30)
    element = EC.presence_of_element_located((By.CLASS_NAME , by_name))
    wait.until(element)
    html = driver.page_source
    soup = BeautifulSoup(html)
    return soup

def soup_wait_clickable(by_name):
    wait = WebDriverWait(driver, 30)
    element = EC.element_to_be_clickable((By.XPATH, by_name))
    wait.until(element)
    html = driver.page_source
    soup = BeautifulSoup(html)
    return soup


if __name__ == "__main__":
    page_set = get_page_set()

    open_links(page_set)
    driver.close()
    driver.switch_to.window(driver.window_handles[-1])
    time.sleep(7)
    for page in page_set:
        print(page)
        get_data()
    print(df)

    #conn = accsql.connecting()
    #accsql.dataInsertPsycopg2(conn, data=df)

    #accsql.accGBQ(df)