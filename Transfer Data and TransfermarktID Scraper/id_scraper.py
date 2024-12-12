"""
This script scrapes player data, including names, queries, and Transfermarkt IDs, from the Transfermarkt website.
It uses a multithreaded approach with `ThreadPoolExecutor` to optimize web scraping and processes large datasets efficiently.

Modules Used:
--------------
- `concurrent.futures`: For multithreaded execution.
- `tqdm`: For progress bars during execution.
- `time`: For handling delays.
- `pandas`: For managing and processing tabular data.
- `bs4.BeautifulSoup`: For HTML parsing.
- `requests`: For making HTTP requests.
"""
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
import time
import pandas as pd
from bs4 import BeautifulSoup
import requests

results = []
def func(i,name):
    """
      Updates the global `results` list with player data processed by the `process_name` function.

      :param i: int, Index in the global `results` list.
      :param name: str, Player name to process.
      :return: None
      """

    global results
    result = process_name(name)
    if result:
        if "Name" in result.keys():
            results[i][0] = result["Name"]
        if "Query" in result.keys():
            results[i][1] = result["Query"]
        if "TransfermarktId" in result.keys():
            results[i][2] = result["TransfermarktId"]


def run_id_scraper(df):
    """
    Scrapes Transfermarkt IDs for players listed in a DataFrame and saves the results to a CSV file.

    :param df: pandas.DataFrame, A DataFrame containing a "Name" column with player names.
    :return: None
    """
    global results
    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = {}
        for name in tqdm(df["Name"][12000:]):

            futures[executor.submit(process_name, name)] = name
            time.sleep(0.08)

        for future in tqdm(as_completed(futures), total=len(futures)):
            try:
                result = future.result()
                if result:
                    results.append(result)

            except Exception as e:
                print(f"Error processing {futures[future]}: {e}")

    # Convert results into a DataFrame
    results_df = pd.DataFrame(results)
    results_df.to_csv("transfermarktId.csv",mode = "a", index=False)
    print(results_df)

headers = {
 'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
}
stop_event = False

class PageLoadTimeoutException(Exception):
    pass


def scrapeTransfermarktId(fullName):
    """
    Scrapes Transfermarkt website for player details using their full name.

    :param fullName: str, Full name of the player.
    :return: tuple or None, A tuple containing the query string and Transfermarkt ID if found,
             otherwise None.
    """
    query_string = fullName.replace(" ", "+")

    response = requests.get("https://www.transfermarkt.com/schnellsuche/ergebnis/schnellsuche?query=" + query_string,
                            headers=headers)
    if not response.status_code == 200:
        print(response.status_code)

    soup = BeautifulSoup(response.content, "html.parser")
    indexes = soup.findAll("a", attrs={"title": fullName})
    if len(indexes) < 1:
        return
    href = indexes[0].get("href")
    string_name = href.split("/")[1]
    string_id = href.split("/")[4]
    return string_name, string_id

def process_name(name):
    """
    Processes a player's name to retrieve their details from Transfermarkt.

    :param name: str, Player's name to process.
    :return: dict or None, A dictionary containing the player's name, query string,
             and Transfermarkt ID, or None if not found.
    """
    list_ = scrapeTransfermarktId(name)
    if not list_:
        return None  # Skip if nothing is returned
    result = {"Name": name}
    if len(list_) > 0:
        result["Query"] = list_[0]
    if len(list_) > 1:
        result["TransfermarktId"] = list_[1]
    return result

p = 'C:\\Users\\alkan\\.cache\\kagglehub\\datasets\\artimous\\complete-fifa-2017-player-dataset-global\\versions\\5\\PlayerNames.csv'
p = p.replace("\\\\", "\\")  # Replace double backslashes with single backslash
df = pd.read_csv(p)
run_id_scraper(df)
