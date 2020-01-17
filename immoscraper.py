#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import json
import os
from concurrent.futures import as_completed
from datetime import datetime

import bs4 as bs
import pandas as pd
import requests
from requests_futures.sessions import FuturesSession


# loads result page and extract the number of search result pages
def get_page_count(result):
    return (
        bs.BeautifulSoup(result.text, "lxml")
            .find("select", {"class": "select font-standard"})
            .find_all("option")[-1]
            .text
    )


# TODO: add argparser to set these values using arguments
search_location = "Berlin"
dump_folder = "data"

base_url = "https://www.immobilienscout24.de"
search_location_data = json.loads(
    requests.get(
        base_url + "/geoautocomplete/v3/locations.json?i=" + search_location
    ).text  # get location search results
)

# get the URL of first the entry of location search results,
# this is needed to limit the scraping to that location
search_url = (
        base_url
        + "/Suche"
        + search_location_data[0]["entity"]["geopath"]["uri"].split("?")[0]
        + "/wohnung-mieten?sorting=2"
          "&pagenumber="
)

# save each expose entry in a dataframe to later dump it as csv
main_dataframe = pd.DataFrame()

# get initial pagecount
mainpage_result = requests.get(search_url)
page_count = get_page_count(mainpage_result)

#  load all search result pages asynchronously
print("going through " + page_count + " pages")
with FuturesSession(max_workers=20) as page_session:
    page_requests = (
        page_session.get(url)
        for url in (
        search_url + str(page) for page in range(1, int(page_count))
    )
    )
    page_results = [request.result() for request in as_completed(page_requests)]

# go through each result page and save the relative expose URL
for result_page_number, result_page in enumerate(page_results, start=1):
    expose_part_urls = []
    main_dataframe = pd.DataFrame()
    print("going through " + result_page.url)  # DEBUG
    soup = bs.BeautifulSoup(result_page.text, "lxml")
    for expose_page_element in soup.find_all(
            "a", {"class": "result-list-entry__brand-title-container"}
    ):
        expose_part_url = expose_page_element.get("href").split("#")[
            0
        ]  # we don't need the anchor of the relative URL
        if (
                r"/expose/" in expose_part_url
        ):  # sometimes an expose is featured in the search results and directs to a page
            # which should be scrapped differently. Thus the entry os doscarded.
            expose_part_urls.append(expose_part_url)

    print(
        "getting exposés of page " + str(result_page_number) + "/" + page_count
    )
    expose_part_urls = list(
        set(expose_part_urls)
    )  # deduplicate relative expose URLs just to be sure

    expose_urls = [
        base_url + expose_url_part for expose_url_part in expose_part_urls
    ]  # make relative URLs absolute

    # load all exposes of a search result page asynchronously
    print("loading " + str(len(expose_part_urls)) + " exposés")
    with FuturesSession(max_workers=100) as expose_session:
        expose_requests = [
            expose_session.get(expose_url) for expose_url in expose_urls
        ]
        expose_results = [
            expose_request.result()
            for expose_request in as_completed(expose_requests)
        ]

    # go through each expose, extract data and save it in DataFrame
    for expose_result in expose_results:
        soup = bs.BeautifulSoup(expose_result.text, "lxml")
        data = pd.DataFrame(
            json.loads(
                str(soup.find_all("script"))
                .split("keyValues = ")[1]
                .split("}")[0]
                + "}"
            ),
            index=[str(datetime.now())],
        )
        data["URL"] = str(expose_result.url)
        description = []
        for description_part in soup.find_all(
                "pre"
        ):  # the description is splitted in multiple, "pre"-tags
            description.append(
                description_part.text
            )  # which are then combined to form the full description
        data["Beschreibung"] = str(description)
        main_dataframe = main_dataframe.append(
            data, sort=False
        )  # add expose data to the main DataFrame

    # export gathered data to disk
    # exposes are grouped by search result page and saved as a csv file
    print("exporting data to disk")
    os.makedirs("data", exist_ok=True)
    main_dataframe.to_csv(
        dump_folder + "/"
        + datetime.isoformat(datetime.now(), timespec="milliseconds")
        .replace(":", "")
        .replace(".", "")
        + ".csv",
        sep=";",
        decimal=",",
        encoding="utf-8",
        index_label="timestamp",
    )
