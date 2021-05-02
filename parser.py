import requests
import aiohttp
import asyncio
import json
import re
import unicodedata
import psycopg2
import configparser
import os
from bs4 import BeautifulSoup as soup
from statistics import mean
from datetime import datetime

import locale
locale.setlocale(locale.LC_ALL, 'ru_RU')
# TODO: Logging
HEADERS = {
            "User-agent": 
            "Mozilla/5.0 (compatible; YandexAccessibilityBot/3.0; +http://yandex.com/bots)"
          }

BASE_DIR = os.path.split(os.path.abspath(__file__))[0]

config = configparser.ConfigParser()
config.read(os.path.join(BASE_DIR, 'config.cfg'))
URL = config['SCRAP'].get('urlfmt')
MAX_CONQUR_REQ = int(config['SCRAP'].get('maxreq', 10))
DB_AUTH = {key: config['DB'][key] for key in config['DB']}

class DateEncoder(json.JSONEncoder):
    def default(self, date, fmt='%Y-%m-%d'):
        if isinstance(date, datetime):
            return datetime.strftime(date, fmt)
        elif isinstance(date, bytes):
            return list(date)
        return json.JSONEncoder.default(self, date)


async def fetch(session, url):
    async with session.get(url) as response:
         await asyncio.sleep(.5)
         return await response.text()


async def fetch_all(urls, loop):
    async with aiohttp.ClientSession(loop=loop, headers=HEADERS) as session:
        results = await asyncio.gather(*[fetch(session, url) for url in urls], return_exceptions=True)
    return results


def get_pages_count(url):
    request = requests.get(url, headers=HEADERS)
    doc = soup(request.text, 'html5lib')
    max_count = max(map(int, (page_num.a.text for page_num in doc.find_all('span',
                                                          {'class':'pager-item-not-in-short-range'}))))
    return max_count


def gen_pages_urls(url, urls_count):
    for i in range(urls_count):
        yield url + f'&page={i}' * (i > 0)


def parse_vacancy_urls(html):
    doc = soup(html, 'html5lib')
    vacancy_serp = doc.find_all('div', {'class': 'vacancy-serp-item'})
    return [vacancy.a['href'].split('?')[0] for vacancy in vacancy_serp]


async def parse_vacancy_data(keyword, html):
    """Get vacancy data and serialize it to json"""
    
    doc = soup(html, 'html5lib')
    vacancy_name = doc.h1.text
    
    try:
        vacancy_salary = mean(map(int, re.findall('\d+', re.sub('\s+', '', unicodedata.normalize("NFKD",
                                                    doc.find('p', {'class':'vacancy-salary'}).text)))))
    except:
        vacancy_salary = None
    
    company_name = doc.select('.vacancy-company__details')[0].text
    
    try:
        address_line = doc.find('div', 'vacancy-company__details').find_next_sibling().text
        company_address = address_line
        # if doc.find('i', {'class': 'icon-pin icon-pin_vacancy-address'}):
        #    company_metro, company_city, _ = address_line.text.partition('Москва')
        #    doc.find('span', {'data-qa': 'vacancy-view-raw-address'}).text.partition('Москва')
        #    company_metro = list(set(company_metro.strip(', ').split(', ')))
        #else:
        #    company_city, _, company_metro = address_line.text.partition(', ')
        #    # doc.find('p', {'data-qa': 'vacancy-view-location'}).text.partition(', ')
    except:
        company_address = ''
    
    vacancy_url = doc.find('link', {'rel': 'canonical'})['href'] \
                              or doc.select('.vacancy-company__details')[0].find('a')['href']
    try:
        vacancy_experience = min(map(int, re.findall('\d',
                                          doc.find('span', {'data-qa': 'vacancy-experience'}).text)))
    except:
        vacancy_experience = 1
    
    try:
        vacancy_description = doc.find('div', {'class': 'vacancy-branded-user-content'}).text
    except:
        vacancy_description = doc.find('div', {'class': 'g-user-content'}).text
    
    try:
        vacancy_skills = [skill.text for skill in
                                    doc.find_all('span', {'class':'bloko-tag__section bloko-tag__section_text'})]
    except:
        vacancy_skills = []
    
    publication_date = datetime.strptime(re.search('\d{1,2} \w+ \d{4}', unicodedata.normalize("NFKD",
                                                    doc.select('.vacancy-creation-time')[0].text))[0], '%d %B %Y')
    
    return json.dumps({
                      'id': int(vacancy_url.rsplit('/')[-1]),
                      'vacancy': vacancy_name,
                      'url': vacancy_url,
                      'salary': vacancy_salary,
                      'company': company_name,
                      'address': company_address,
                      # 'metro': company_metro if company_metro else [],
                      'experience': vacancy_experience,
                      'description': vacancy_description,
                      'skills': vacancy_skills,
                      'publication_date': publication_date,
                      'keyword': keyword
          }, ensure_ascii=False, cls=DateEncoder)


async def parse_vacancies(keyword, htmls):
    results = await asyncio.gather(*[parse_vacancy_data(keyword, html) for html in htmls], return_exceptions=True)
    return results


def connect_to_db(auth):
    try:
        conn = psycopg2.connect(**auth)
        return conn
    except psycopg2.OperationalError as e:
        print(f"Database error {e}")
    return False


def table_insert(data, conn):
    """CREATE TABLE IF NOT EXISTS vacancies (
        id int PRIMARY KEY  NOT NULL,
        vacancy varchar(150) NOT NULL,
        company varchar(150) NOT NULL,
        experience int,
        address varchar(250),
        salary int,
        url varchar(250) NOT NULL,
        description text,
        skills varchar[],        
        date date,
        keyword varchar(80)
       );
    """
    insert_query = """INSERT INTO vacancies (
                        id, vacancy, company, experience, address,
                        salary, url, description, skills, date, keyword
                      )
                      VALUES (
                        %(id)s, %(vacancy)s, %(company)s, %(experience)s,
                        %(address)s, %(salary)s, %(url)s, %(description)s,
                        %(skills)s, DATE %(publication_date)s, %(keyword)s
                      )
                      ON CONFLICT (id) DO UPDATE SET
                        vacancy = EXCLUDED.vacancy
                        , date = EXCLUDED.date
                        , address = EXCLUDED.address
                        , salary = EXCLUDED.salary
                        , skills = EXCLUDED.skills;
                      """
    
    if conn:
        with conn, (cursor:=conn.cursor()):
            cursor.executemany(insert_query, map(json.loads, data))
            conn.commit()
            count = cursor.rowcount
            status = cursor.statusmessage
            plural = count > 1
            print(f"{count} row{'s' * plural} {['is', 'are'][plural]} {status.split()[0].lower()}ed into the table")
    else:
        print("Database is not connected")
    #cursor.close()


def main(keyword, items_on_page=100, area=113):
    """ area 1 - Moscow, 113 - Russia; keyword - search query; items_on_page - vacancies on a serp """
    
    conn = connect_to_db(DB_AUTH)
    # url = f'https://hh.ru/search/vacancy?st=searchVacancy&text={keyword} \
    #                                                     &area={area} \
    #                                                     &items_on_page={items_on_page}'
    
    url = URL.format(keyword, area, items_on_page) # eval(f"f'{URL}'")
    urls = [*gen_pages_urls(url, get_pages_count(url))]
    print(len(urls))
    loop = asyncio.get_event_loop()
    htmls = loop.run_until_complete(fetch_all(urls, loop))
    vacancy_urls = sum([parse_vacancy_urls(html) for html in htmls if not isinstance(html, Exception)], [])
    
    for i in range(len(vacancy_urls) // (n := MAX_CONQUR_REQ)):
        htmls = loop.run_until_complete(fetch_all(vacancy_urls[i * n: i * n + n], loop))
        results = loop.run_until_complete(parse_vacancies(keyword,
                                               [html for html in htmls if not isinstance(html, Exception)]))
        #print(r)
        table_insert([res for res in results if not isinstance(res, Exception)], conn)

if __name__ == '__main__':
    #url = 'https://hh.ru/search/vacancy?st=searchVacancy&text=python&area=1&items_on_page=100'
    
    #urls = get_pages_urls(url, 2)
    #loop = asyncio.get_event_loop()
    #htmls = loop.run_until_complete(fetch_all(urls, loop))
    #vacancy_urls = sum([parse_vacancy_urls(html) for html in htmls if not isinstance(html, Exception)], [])
     
    area = config['SCRAP'].get('area', 1)
    query =  area = config['SCRAP'].get('query', 'data+analyst')
    main(query, area)

    #htmls = loop.run_until_complete(fetch_all(vacancy_urls[:2], loop))
    #print(parse_vacancy_data(htmls2[:5]))
    #print([parse_vacancy_data(html) for html in htmls if not isinstance(html, Exception)])
    #r = loop.run_until_complete(parse_vacancies([html for html in htmls if not isinstance(html, Exception)]))
    #print(r)
    
    #for i in range(len(vacancy_urls[:5]) // (n:= MAX_CONQUR_REQ)):
    #    htmls = loop.run_until_complete(fetch_all(vacancy_urls[i * n: i * n + n], loop))
    #    r = loop.run_until_complete(parse_vacancies([html for html in htmls if not isinstance(html, Exception)]))
    
    #     tasks = [asyncio.create_task(parse_vacancies([html for html in htmls if not isinstance(html, Exception)]))]
    #     await asyncio.gather(*tasks)
        #save_sqlite([task.result() for task in tasks])
    #     print([task.result() for task in tasks])
    #    print(r)
