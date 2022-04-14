
import os
from dotenv import load_dotenv
import logging
from bs4 import BeautifulSoup
import urllib.request
import csv
from markupsafe import string
import pandas as pd
from datetime import date
from pymongo import MongoClient
import time


logging.basicConfig(filename="crawler.log",
                    format='%(asctime)s %(message)s',
                    filemode='w')

logger = logging.getLogger()
logger.setLevel(logging.DEBUG)


def find_first(page, element, tag, name, printText=False):
    """
    Find first {element} in {page} page who contains specific {tag} and {name}
    """
    try:
        result = page.find(element, {tag: name})
        if not printText:
            return result
        return result.text
    except AttributeError as err:
        logger.debug(err)
        return ""


def find_all(page, element: string, tag: string, name: string, printText=False):
    """
    Find all {element} in html page who contains specific {tag} and {name}
    """
    try:
        result = page.find_all(element, {tag: name})
        if not printText:
            return result
        return result.text
    except AttributeError as err:
        logger.debug(err)
        return ""


def find_first_basic_element(page, element: string, printText=False):
    """
    Find first basic {element} in html {page}
    exemple: if {element} == 'div' -> return first div
    """
    try:
        result = page.find(element)
        if not printText:
            return result
        return result.text
    except AttributeError as err:
        logger.debug(err)
        return ""


def find_all_basic_element(page, element: string, printText=False):
    """
    Find all basic {element} in {page}
    exemple: if {element} == 'div' -> return all divs
    """
    try:
        result = page.find_all(element)
        if not printText:
            return result
        return result.text
    except AttributeError as Exception:
        logger.debug(Exception)
        return ""


def init_bs4(baseUrl: string, read='lxml'):
    """
    Init BeautifulSoup page
    """
    url = urllib.request.urlopen(baseUrl).read()
    soup = BeautifulSoup(url, read)
    return soup


class Crawling():
    def __init__(self):
        """
        Init url / page and csv_output
        """
        self.url = os.getenv('URL_CRAWLING')
        self.soup = init_bs4(self.url)
        outfile = open(os.getenv('CSV_CRAWLING'), 'w', newline='')
        self.writer = csv.writer(outfile)
        self.writer.writerow(['link'])

    def get_horses_link(self):
        list_classname = ['trOne', 'trTwo']
        for classname in list_classname:
            try:
                for element in find_all(self.soup, 'tr', 'class', classname):
                    links = find_all_basic_element(element, 'a')
                    for link in links:
                        yield link['href']
            except Exception as err:
                logger.debug(err)

    def save_link_to_csv(self, link: string):
        """
        Store {link} into csv file
        """
        self.writer.writerow([link])

    def main(self):
        for link in self.get_horses_link():
            self.save_link_to_csv(link.replace(".html?idcheval=", "_"))


class Scrapping():
    def __init__(self):
        """
        Init url / page and csv_output
        """
        self.init_csv()
        self.mongo = self.init_mongo()
        self.links = pd.read_csv(os.getenv('CSV_CRAWLING'))
        self.dicto = {'Name': '', 'Gains': '', 'Courues': '', 'Victoires': '',
                      'Placés': '', 'Sexe': "", "Age": ""}

    def get_horse_name(self, page):
        """
        Return the name of the horse
        """
        return find_first(page, 'h1', "class", "fiche", printText=True)

    def init_csv(self):
        """
        Set csv file
        """
        try:
            outfile = open(os.getenv('CSV_SCRAPING'), 'w', newline='')
            self.writer = csv.writer(outfile)
            self.writer.writerow(['Name', 'Gains', 'Courues', 'Victoires', 'Placés', 'Sexe', 'Age'])
        except Exception as err:
            logger.debug(err)

    def init_mongo(self):
        """
        Set MongoDB connexion
        """
        try:
            conn = MongoClient(os.getenv('MONGO_CLIENT'))
            db = conn[os.getenv('MONGO_DATABASE')]
            self.collection = db.horse
            return self.collection
        except Exception as err:
            logger.debug(err)

    def save_output(self, csv: bool, mongo: bool, horseName: string): 
        def save_output_in_mongo(horseName: string):
            mongo_data = {
                "horseName": horseName,
                "Gains": self.dicto['Gains'],
                "Courues": self.dicto['Courues'],
                "Victoires": self.dicto['Victoires'],
                "Placés": self.dicto['Placés'],
                "Sexe": self.dicto['Sexe'],
                "Age": self.dicto['Age'],
                "createdAt": date.today().strftime("%d/%m/%Y")
            }
            self.mongo.insert_one(mongo_data)

        def save_output_in_csv(horseName: string):
            self.writer.writerow([horseName, self.dicto['Gains'], self.dicto['Courues'], self.dicto['Victoires'],
            self.dicto['Placés'], self.dicto['Sexe'], self.dicto['Age']])

        if csv or mongo:
            self.clean_output(self.dicto)           
        if csv:
            save_output_in_csv(horseName=horseName)
        if mongo:
            save_output_in_mongo(horseName=horseName)

    def get_all_informations(self, page):
        """
        Yield element if it is present in array
        """
        body = page.find('tbody')
        for tr in find_all_basic_element(body, 'tr'):
            find_all_td = find_all_basic_element(tr, 'td')
            for i in range(0, len(find_all_td)):
                if any(ele in find_all_td[i].text for ele in ['Gains', 'Sexe', 'Courues', 'Age', 'Victoires', 'Placés']):
                    yield find_all_td[i].text, find_all_td[i+1].text

    def clean_output(self, output: dict):
        for key, value in output.items():
            if value == "-":
                self.dicto[key] = "0"
            elif "ans" in value:
                self.dicto[key] = value.replace("ans", "")
            elif "€" in value:
                self.dicto[key] = value.replace("€", "")


    def main(self):

        for i in range(len(self.links)):

            soup = init_bs4(os.getenv('BASE_URL_SCRAPING') + self.links.loc[i, "link"])
            horseName = self.get_horse_name(soup)

            for element, value in self.get_all_informations(soup):
                self.dicto[element.split(" ", 1)[0]] = value
            self.save_output(csv=True, mongo=True, horseName=horseName)
            self.dicto.clear()


if __name__ == '__main__':
    load_dotenv()

    #crawler = Crawling()
    #crawler.main()
    #time.sleep(5)
    scraping = Scrapping()
    scraping.main()
