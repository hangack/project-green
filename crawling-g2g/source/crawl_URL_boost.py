# /e/Fear/Univ/Big_data/Training/python/python-crawling/venv/Scripts/python
# -*- encoding: utf-8 -*-

import pandas as pd
import pandas_gbq
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import time
import re
import datetime
import psycopg2
from psycopg2 import connect, extensions
import psycopg2.extras as extras
import glob
from google.oauth2 import service_account

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

    def get_data_seller(selector):
        seller_list = soup.select(selector)
        seller_list_text = []
        for seller in seller_list:
            seller_list_text.append(seller.string)
        return seller_list_text

    def get_data_stock():
        option_list = soup.select("div.offers-bottom-attributes.offer__content-lower-items > span")
        stock_list = []
        n = 0
        for option in option_list:
            n += 1
            if n%3 != 2:
                continue
            stock_list.append(option.string)
        return stock_list


    driver.switch_to.window(driver.window_handles[-1])
    class_name = "offers-bottom-attributes.offer__content-lower-items"
    soup = soup_wait(class_name)

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

    seller.extend(get_data_seller("a.flex.prechekout-non-produdct-details > div.seller-details.m-l-sm > div.seller__name-detail"))
    price.extend(get_data_seller("div.hide > div > div > div > div > div > span.offer-price-amount"))
    stock.extend(get_data_stock())


    i = 1
    xpath_name = '//a[@href="#!-1"]'
    if bool(soup.find("a", class_="cdp_i")):
        while True:
            i += 1
            print("page"+str(i))
            driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            driver.find_element_by_xpath('//a[@href="#!+1"]').click()
            time.sleep(1)
            soup = soup_wait_clickable(xpath_name)

            seller.extend(get_data_seller("a.flex.prechekout-non-produdct-details > div.seller-details.m-l-sm > div.seller__name-detail"))
            price.extend(get_data_seller("div.hide > div > div > div > div > div > span.offer-price-amount"))
            stock.extend(get_data_stock())

            cdp_i = soup.find_all("a", class_="cdp_i")
            cdp = soup.find("div", class_="content_detail__pagination cdp")
            cdp = str(cdp)
            actpage = int(re.search('actpage="(.+?)" class', cdp).group(1))
            if actpage == (len(cdp_i)-2):
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


def connecting():
    # DB Connect
    conn = connect(
        host = "localhost",
        dbname = "db_g2g",
        user = "postgres",
        password = "pwd",
        port = "5432"
    )

    return conn

def dataInsertPsycopg2(conn, data):
    # Single Insert
    TABLE_NAME = "poe"

    # AutoCommit
    autocommit = extensions.ISOLATION_LEVEL_AUTOCOMMIT
    print("ISOLATION_LEVEL_AUTOCOMMIT:", extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    conn.set_isolation_level(autocommit)

    tuples = [tuple(x) for x in data.to_numpy()]

    cols = ','.join(list(data.columns))
    print(cols) # STATION,GETON_PPL,GETOFF_PPL


    # SQL query to execute
    query = "INSERT INTO %s(%s) VALUES %%s" % (TABLE_NAME, cols)
    print(query)


    cursor = conn.cursor()
    # https://www.psycopg.org/docs/extras.html
    try:
        extras.execute_values(cursor, query, argslist = tuples)
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error: %s" % error)
        conn.rollback()
        cursor.close()
        return 1
    print("the dataframe is inserted")
    cursor.close()
    conn.close()


def accGBQ():
    #key_path = glob.glob('E:/Fear/Univ/Big_data/Training/python/python-crawling/g2g/g2g-crawling-96ce5be30687.json')[0]
    credentials = service_account.Credentials.from_service_account_info(
        {
            "type": "",
            "project_id": "",
            "private_key_id": "",
            "private_key": "",
            "client_email": "",
            "client_id": "",
            "auth_uri": "",
            "token_uri": "",
            "auth_provider_x509_cert_url": "",
            "client_x509_cert_url": ""
        }
    )

    pandas_gbq.context.credentials = credentials
    pandas_gbq.context.project = "g2g-crawling"

    table_name = "DB_G2G.poe"
    project_id = "g2g-crawling"

    pandas_gbq.to_gbq(df, table_name, project_id=project_id, if_exists="append")

    print('to_GBQ: migration complete')


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

    conn = connecting()
    dataInsertPsycopg2(conn, df)

    accGBQ()