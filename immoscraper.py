#!/usr/bin/env python
# -*- coding: utf-8 -*-
import json
import os
from concurrent.futures import as_completed
from datetime import datetime

import bs4 as bs
import pandas as pd
import requests
from requests_futures.sessions import FuturesSession

search_location = "Berlin"
base_url = "https://www.immobilienscout24.de"
search_location_data = json.loads(
    requests.get("https://www.immobilienscout24.de/geoautocomplete/v3/locations.json?i=" + search_location).text
)

search_url = (
    base_url
    + "/Suche"
    + search_location_data[0]["entity"]["geopath"]["uri"].split("?")[0]
    + "/wohnung-mieten?sorting=2"
    "&pagenumber="
)

df = pd.DataFrame()

mainpage_result = requests.get(search_url)

page_count = (
    bs.BeautifulSoup(mainpage_result.text, "lxml")
    .find("select", {"class": "select font-standard"})
    .find_all("option")[-1]
    .text
)

print("going through " + page_count + " pages")
# bulk load all pages
with FuturesSession(max_workers=20) as page_session:
    page_requests = (page_session.get(url) for url in (search_url + str(page) for page in range(1, int(page_count))))
    page_results = [request.result() for request in as_completed(page_requests)]

# go through each page
for result_page_number, result_page in enumerate(page_results, start=1):
    expose_part_urls = []
    df = pd.DataFrame()
    print("going through " + result_page.url)  # DEBUG
    soup = bs.BeautifulSoup(result_page.text, "lxml")
    for expose_page_element in soup.find_all("a", {"class": "result-list-entry__brand-title-container"}):
        expose_part_url = expose_page_element.get("href").split("#")[0]
        if r"/expose/" in expose_part_url:  # sometimes other entries (ads) are found that are structured differently
            expose_part_urls.append(expose_part_url)

    print("getting exposés of page " + str(result_page_number) + "/" + page_count)
    expose_part_urls = list(set(expose_part_urls))  # gather the urls of all exposes of a page and deduplicate them

    # bulk load all exposes of a page
    print("loading " + str(len(expose_part_urls)) + " exposés")
    with FuturesSession(max_workers=100) as expose_session:
        expose_urls = [base_url + expose_url_part for expose_url_part in expose_part_urls]
        expose_requests = [expose_session.get(expose_url) for expose_url in expose_urls]
        expose_results = [expose_request.result() for expose_request in as_completed(expose_requests)]
    # go through each expose
    for expose_result in expose_results:
        soup = bs.BeautifulSoup(expose_result.text, "lxml")
        data = pd.DataFrame(
            json.loads(str(soup.find_all("script")).split("keyValues = ")[1].split("}")[0] + "}"),
            index=[str(datetime.now())],
        )
        data["URL"] = str(expose_result.url)
        description = []
        for i in soup.find_all("pre"):
            description.append(i.text)
        data["Beschreibung"] = str(description)
        df = df.append(data, sort=False)

    print("exporting data to disk")
    os.makedirs("data", exist_ok=True)
    df.to_csv(
        "data/"
        + datetime.isoformat(datetime.now(), timespec="milliseconds").replace(":", "").replace(".", "")
        + ".csv",
        sep=";",
        decimal=",",
        encoding="utf-8",
        index_label="timestamp",
    )
