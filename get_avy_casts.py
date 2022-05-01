import requests
from bs4 import BeautifulSoup
URL = 'https://www.sierraavalanchecenter.org/prior-jan-6-2021/archive'

page = requests.get(URL)
soup = BeautifulSoup(page.content, "html.parser")
