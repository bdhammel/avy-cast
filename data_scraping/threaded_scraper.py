import datetime
import logging
import re
import threading
from concurrent.futures import ThreadPoolExecutor
from queue import Full, Queue

import pandas as pd
import requests
from bs4 import BeautifulSoup

BASE_URL = "https://www.sierraavalanchecenter.org"
URL = "/prior-jan-6-2021/archive"

prog = re.compile(r"(\d+\-\d+\-\d+)$")
problem_filter = re.compile(r"Avalanche Problem \d: ([\w\s]*)")

logging.basicConfig(level=logging.INFO, format="%(thread)d--\t %(message)s")


def get_rating(img) -> str:
    rating = img.get("src").split("/")[-1]
    ratings = {
        "1.png": "low",
        "2.png": "moderate",
        "3.png": "considerable",
        "4.png": "high",
        "5.png": "extreme",
        "0.png": "na",
        "6.png": "na",
    }
    try:
        rating = ratings[rating]
    except KeyError:
        rating = None
        logging.error(f"Didn't fine a rating for input {rating}")
    return rating


def consumer(queue: Queue, finished: threading.Event, data: list):
    logger = logging.getLogger("consumer")
    while not queue.empty() or not finished.is_set():
        try:
            date, rating, url = queue.get(timeout=5)
        except queue.Empty:
            logger.exception("queue timeout on get")

        logger.info(f"\t processing problem url for date {date}. danger lvl {rating}")
        logger.debug(f"\t {url}")

        problem_types = []

        page = requests.get(url)

        problem_soup = BeautifulSoup(page.content, "html.parser")
        problems = problem_soup.find_all(text=problem_filter)
        for problem in problems:
            m = re.findall(problem_filter, problem)
            problem_types.append(m[0])

        datum = (date, rating, url, tuple(problem_types))
        data.append(datum)


def producer(queue: Queue, finished: threading.Event):
    logger = logging.getLogger("producer")
    url = BASE_URL + URL
    while url:
        logger.info(f"processing page {url}")
        page = requests.get(url)
        soup = BeautifulSoup(page.content, "html.parser")
        rows = soup.find("tbody").find_all("tr")
        for row in rows:
            td = row.find("td")
            report = td.find("a").get("href")
            date_str = prog.findall(td.find("strong").text)[0]
            date = datetime.datetime.strptime(date_str, "%Y-%m-%d")
            img = td.find("img")
            rating = get_rating(img)

            if rating is None:
                logger.info(f"No rating for date {date}")
                continue

            problem_url = BASE_URL + report
            data_packet = (date, rating, problem_url)

            # send the url with the problems to be processed by the consumer pool
            logger.info(f"Sending {problem_url} to be processed")
            try:
                queue.put(data_packet, timeout=5)
            except Full:
                logger.exception("could not place item in queue")
                finished.set()
                return

        try:
            next_url = soup.find("li", class_="pager-next").find("a").get("href")
        except AttributeError:
            next_url = False
        break
        url = next_url
    finished.set()


if __name__ == "__main__":
    data = []
    queue = Queue()  #  maxsize=10)
    finished = threading.Event()
    threads = []
    workers = 10
    with ThreadPoolExecutor(max_workers=workers) as executor:
        threads.append(executor.submit(producer, queue, finished))
        for _ in range(workers - 1):
            threads.append(executor.submit(consumer, queue, finished, data))

    dates, ratings, urls, problems = zip(*data)
    df = pd.DataFrame(
        {"date": dates, "rating": ratings, "url": urls, "problem": problems}
    )
