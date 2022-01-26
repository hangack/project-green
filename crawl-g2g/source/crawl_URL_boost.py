# /e/Fear/Univ/Big_data/Training/python/python-crawling/venv/Scripts/python
# -*- encoding: utf-8 -*-

import pandas as pd
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import datetime
import psycopg2
from psycopg2 import connect, extensions
import psycopg2.extras as extras

pd.set_option("display.max_rows", 200)
pd.set_option("display.max_columns", 200)
pd.set_option("display.width", 100)

options = webdriver.ChromeOptions()
options.add_argument("headless")
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
    driver.switch_to.window(driver.window_handles[-1])
    class_name = "seller__name"
    soup = soup_wait(class_name)

    today = datetime.date.today().strftime("%Y-%m-%d")

    seller = soup.find("a", class_ = "seller__name").get_text()

    region_right_tolist = soup.select("div.region_right-detail")
    data_col = []
    for list in region_right_tolist:
        data_col.append(list.string)

    price = soup.find("span", id = "precheckout_ppu_amount").get_text()
    stock = soup.find("span", id = "precheckout_offer_stock").get_text()

    data = [today,seller,data_col[0][49:-44],data_col[1][49:-44],price,stock]

    driver.close()

    return data


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


def connecting():
    # DB Connect
    conn = connect(
        host = "localhost",
        dbname = "g2gtestdb",
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


if __name__ == "__main__":
    page_set = get_page_set()

    open_links(page_set)
    driver.close()
    for page in page_set:
        df.loc[len(df)] = get_data()
    print(df)

    conn = connecting()
    dataInsertPsycopg2(conn, df)

    df.to_gbq