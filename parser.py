import re
import csv
import time
import requests
import asyncio
import aiohttp
from random import choice
from datetime import datetime
from bs4 import BeautifulSoup as bs
from fake_useragent import UserAgent

proxy_list = open("http_proxies.txt").readlines()


class Parser:
    def __init__(self):
        self.category_links = []
        self.page_links = []
        self.drink_links = []
        self.csv_rows = []
        self.csv_name = f"{datetime.today().strftime('%Y-%m-%d')}/{datetime.now().strftime('%H:%M:%S')}"
        self.basic_url = f"https://simplewine.ru"
        self.ua = UserAgent()
        self.headers = {
            "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9",
            "user-agent": self.ua.random,
            # "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/94.0.4606.61 Safari/537.36",
        }

    def parse_categories(self):
        response = requests.get(url=self.basic_url, headers=self.headers)
        soup = bs(response.text, "lxml")
        nav = soup.find("ul", class_="navigation__list")
        li_s = nav.find_all("li", class_="navigation__item")
        for index in [0, 1, 4]:
            a = li_s[index].find("a")
            href = a["href"]
            link = f"{self.basic_url}{href}"
            self.category_links.append(link)

    def create_page_links(self):

        self.pages_pc = []
        # print("--------", self.category_links[2:])
        for link in self.category_links:

            numbers = []
            response = requests.get(url=link, headers=self.headers)
            soup = bs(response.text, "lxml")
            pagination = soup.find("div", class_="pagination__navigation")
            a_s = pagination.find_all("a")
            for a in a_s:
                page_num = a.text.strip()
                # print(page_num)
                numbers.append(page_num)

            last = numbers[-2]

            self.pages_pc.append(last)

        for last_page, link in zip(self.pages_pc, self.category_links):
            # first page
            self.page_links.append(link)

            for i in range(2, int(last_page) + 1):
                page_link = f"{link}page{i}/"
                self.page_links.append(page_link)

        print(self.page_links)

    async def get_drinks(self, session, link, page):
        async with session.get(
            url=link,
            headers=self.headers,
        ) as response:
            response = await response.text()

            page_soup = bs(response, "lxml")
            drink_cards = page_soup.find_all(
                "div", class_="catalog-grid__item")

            for card in drink_cards:
                tag = card.find("a")
                href = tag["href"]
                drink_link = f"{self.basic_url}{href}"
                self.drink_links.append(drink_link)
        print(f"parsing drinks on page {page}")

    async def get_drink_info(self, session, link, num):
        try:

            for attempt in range(4):
                try:
                    proxy = choice(proxy_list)
                    async with session.get(
                        url=link,
                        headers=self.headers,
                        proxy=f"http://{proxy}",
                    ) as response:

                        response = await response.text()
                        soup = bs(response, "lxml")

                        drink_info = {}

                        try:
                            container = soup.find(
                                "div", class_="product-card-type-a__header"
                            )
                            h1 = container.find(
                                "h1", class_="product-card-type-a__header-title"
                            )
                            title = str(h1.text.strip())
                            drink_info["title"] = title
                        except:
                            pass

                        try:
                            price = soup.find(
                                "div", class_="product-buy__price"
                            ).text.strip()
                            price = re.findall(r"\d+", price)
                            drink_info["price"] = price
                        except:
                            pass

                        try:
                            discount = soup.find(
                                "div", class_="product-buy__discount"
                            ).text.strip()
                            discount = re.findall(r"\d+", discount)
                            drink_info["discount"] = discount[0]
                        except:
                            pass

                        try:
                            num_scores = soup.find(
                                "p", class_="product-rating__ratings"
                            ).text.strip()
                            drink_info["num_scores"] = num_scores
                        except:
                            pass

                        try:
                            rating = soup.find(
                                "p", class_="product-rating__rating-text"
                            ).text.strip()
                            drink_info["rating"] = rating
                        except:
                            pass

                        drink_info["link"] = link

                        self.csv_rows.append(drink_info)
                        print(f"Got drink {num} with proxy")
                except:
                    continue
                else:
                    break

            else:
                async with session.get(url=link, headers=self.headers) as response:

                    response = await response.text()
                    soup = bs(response, "lxml")

                    drink_info = {}

                    try:
                        container = soup.find(
                            "div", class_="product-card-type-a__header"
                        )
                        h1 = container.find(
                            "h1", class_="product-card-type-a__header-title"
                        )
                        title = str(h1.text.strip())
                        drink_info["title"] = title
                    except:
                        pass

                    try:
                        price = soup.find(
                            "div", class_="product-buy__price"
                        ).text.strip()
                        price = re.findall(r"\d+", price)
                        drink_info["price"] = price
                    except:
                        pass

                    try:
                        discount = soup.find(
                            "div", class_="product-buy__discount"
                        ).text.strip()
                        discount = re.findall(r"\d+", discount)
                        drink_info["discount"] = discount[0]
                    except:
                        pass

                    try:
                        num_scores = soup.find(
                            "p", class_="product-rating__ratings"
                        ).text.strip()
                        drink_info["num_scores"] = num_scores
                    except:
                        pass

                    try:
                        rating = soup.find(
                            "p", class_="product-rating__rating-text"
                        ).text.strip()
                        drink_info["rating"] = rating
                    except:
                        pass

                    drink_info["link"] = link

                    self.csv_rows.append(drink_info)
                    print(f"Got drink {num}")
        except asyncio.TimeoutError:
            print(f"Timeout error for: {link}")
            pass

    async def gather_drinks_tasks(self):
        print("Gathering drinks tasks\n")
        async with aiohttp.ClientSession() as session:

            drinks_tasks = []

            for num, link in enumerate(self.page_links):
                task = asyncio.create_task(self.get_drinks(session, link, num))
                drinks_tasks.append(task)

            await asyncio.gather(*drinks_tasks)

    async def gather_info_tasks(self):
        print("Gathering info tasks\n")
        async with aiohttp.ClientSession(
            connector=aiohttp.TCPConnector(ssl=False, force_close=True)
        ) as session:
            info_tasks = []
            # for link in self.drink_links:
            for num, link in enumerate(self.drink_links):
                task = asyncio.create_task(
                    self.get_drink_info(session, link, num))
                info_tasks.append(task)

            await asyncio.gather(*info_tasks)

    def write_to_csv(self):
        print("Writing csv")

        with open("test.csv", "w", encoding="utf-8") as file:
            headers = ["title", "price", "discount",
                       "num_scores", "rating", "link"]
            w = csv.DictWriter(file, headers)
            w.writeheader()
            for row in self.csv_rows:
                w.writerow(row)

    def main(self):
        start_time = time.time()
        self.parse_categories()
        print(self.category_links)
        self.create_page_links()
        asyncio.run(self.gather_drinks_tasks())
        asyncio.run(self.gather_info_tasks())
        self.write_to_csv()
        finish_time = time.time() - start_time
        print(f"\n\nTime to finish: {finish_time}")


if __name__ == "__main__":

    parser = Parser()
    parser.main()
