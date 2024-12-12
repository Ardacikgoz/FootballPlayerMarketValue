"""
This module scrapes the data of the provided players, collects the transfer and market valuation history
APPENDS them in a csv.
"""

import queue
import timeit
from fake_useragent import UserAgent
from bs4 import BeautifulSoup
import pandas as pd
from selenium import webdriver
from selenium.webdriver.support import expected_conditions as EC
from concurrent.futures import ThreadPoolExecutor, as_completed
from selenium.webdriver.edge.service import Service
from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.common.by import By
import time

stop_event = False# Is used for manually stopping the scraping mid-scrape, currently unavailable

def scrape_transfer_data(query, transfermarktId, driver,thread_no):
    """
    Goes to the webpage of transfer history for the player provided, loads and scrapes the data

    :param query: Query string for a player recorded in the transfermarkt.com, required for accessing their information such as 'cristiano-ronaldo'
    :param transfermarktId: Transfermarkt ID of a player, required to access the webpage related to the player
    :param driver: Driver object created by selenium to access webpages
    :param thread_no: Number of the thread that is working on this function
    :return: A table consisting past transfer data and market valuations from transfermarkt for a player
    """
    print(str(transfermarktId)+" out for : "+str(thread_no)) #This print statement is for tracking the code, prints which thread is attempting to scrape and the player transfermarktid being scraped
    start = time.time()
    out = get(driver,"https://www.transfermarkt.com/{}/transfers/spieler/{}".format(query, transfermarktId))
    print(str(transfermarktId)+" out for: "+str(thread_no)+" in:"+str(time.time()-start)+" "+str(out))#This print statement is for tracking the code, prints which thread is attempting to scrape and the player transfermarktid being scraped, along with how long it took to scrape
    if not out:
        return False
    # Skip to the middle of the page to load dynamic html containing transfer history
    driver.execute_script("window.scrollTo(0, document.body.scrollHeight/(2.3));")
    transfer_history_div = WebDriverWait(driver, 30).until(
        EC.presence_of_element_located((By.XPATH, "//div[@class='grid tm-player-transfer-history-grid']"))
    )
    html = driver.page_source
    soup = BeautifulSoup(html, 'html.parser')
    indexes = soup.findAll("div", attrs={"class": "grid tm-player-transfer-history-grid"})
    seasons = []
    for season in indexes:
        seasonName = season.findAll("div", attrs={"class": "grid__cell grid__cell--center tm-player-transfer-history-grid__season"})[0].text
        date = season.findAll("div", attrs={"class": "grid__cell grid__cell--center tm-player-transfer-history-grid__date"})[0].text
        oldClub = season.findAll("div", attrs={"class": "grid__cell grid__cell--center tm-player-transfer-history-grid__old-club"})[0].text
        newClub = season.findAll("div", attrs={"class": "grid__cell grid__cell--center tm-player-transfer-history-grid__new-club"})[0].text
        marketValue = season.findAll("div", attrs={"class": "grid__cell grid__cell--center tm-player-transfer-history-grid__market-value"})[0].text
        transferFee =season.findAll("div", attrs={"class": "grid__cell grid__cell--center tm-player-transfer-history-grid__fee"})[0].text
        seasons.append([query,seasonName, date, oldClub, newClub, marketValue, transferFee])
    return seasons
result_queue = queue.Queue() # This queue stores the results from scraping processes, built as a queue to avoid racing conditions

total_scraped = queue.Queue() # This queue is used to track the progress by adding one element to the queue every time a data is fetched

def get(driver,url):
    """
    Accesses a webpage, built to add a timeout to each access trial
    :param driver: Driver object from selenium
    :param url: URL to access with driver
    :return: True if the access is successful, False otherwise
    """
    def load_page():
        driver.get(url)

    with ThreadPoolExecutor(max_workers=1) as executor:

        future = executor.submit(load_page)

        try:
            future.result(timeout=20)  # Timeout after x seconds
            result = True
        except TimeoutError:
            result =  False
        finally:
            return result


def try_initialize_driver(thread_no):
    """
    Initializes the driver with statically defined options
    :param thread_no: Number of the thread working on this function
    :return: Driver object if the call was succesful, False otherwise
    """
    options = webdriver.FirefoxOptions()
    ua = UserAgent()
    options.add_argument(f"user-agent={ua.random}")
    options.add_argument('--headless')  # Headless mod
    driver = webdriver.Firefox(options=options,service=Service(r"C:\Users\alkan\Downloads\geckodriver-v0.35.0-win64\geckodriver.exe"))
    try:

        out = get(driver,"https://www.transfermarkt.com")
        if not out:
            print("Task took too long, moving on for: " + str(thread_no))
            return False

        print("got "+str(thread_no))
        wait = WebDriverWait(driver, 20)
        # wait for the accept privacy permissions
        wait.until(EC.frame_to_be_available_and_switch_to_it((By.XPATH, "//iframe[@id='sp_message_iframe_953358']")))
        #print("step1 "+str(thread_no))
        driver.switch_to.default_content()
        frame = driver.find_element(By.XPATH,"//iframe[@id='sp_message_iframe_953358']")
        driver.switch_to.frame(frame)
        wait.until(EC.element_to_be_clickable((By.XPATH, "//*[@title = 'Accept & continue']"))).click()
        #print("step2 "+str(thread_no))
        return driver
    except Exception as e:
        print(str(e)+" "+str(thread_no))
        driver.quit()
        return False

def run_scraper(scrape_list,thread_no):
    """
    Runs all the scraper operations for a thread
    :param scrape_list: The list of players to scrape along with their informations
    :param thread_no: The thread working on this function
    :return: False if the driver initialization fails and terminates, None otherwise
    """
    # Start the driver healthly, accept the cookies and prepare, if not successful, we can retry as much time as the range value in the loop
    for i in range(1):
        driver = try_initialize_driver(thread_no)
        if driver:
            break
    # If driver == false, driver wasn't initialized succesfully, terminate thread
    if not driver:
        return False
    print("initialized {}".format(thread_no))

    time.sleep(2)

    i = 0
    results = []
    global stop_event
    global total_scraped # Total_scraped is a queue, it is used for following the progress by checking the total size of the queue, it means how many player's data was collected
    for row in scrape_list.iterrows():
        if stop_event: # Is used for manually stopping the scraping mid-scrape, currently unavailable
            break
            print("breaking {}".format(thread_no))
        i+=1


        total_scraped.put(1)
        print(str(total_scraped.qsize())+" "+str(thread_no)) # Shows the number of scraped players, is used to track progress
        try:
            # If the player data wasn't fetched successfully, skip to the next player, not the end of the world
            result = scrape_transfer_data(row[1]["Query"],row[1]["TransfermarktId"],driver,thread_no)
            time.sleep(0.08)
            results.extend(result)
        except:
            continue

        #print(f"Iteration : {i}")
    global result_queue
    result_queue.put(results) # Store the fetched data in a queue to avoid racing conditions
    driver.quit() # Don't forget to close drivers
    print("appended {}".format(thread_no))

def force_stop():
    """
    Is used for manually stop scraping mid-scraping, currently unavaliable
    :return: None
    """
    global stop_event
    while not stop_event:
        x = input("Write 'stop' to stop scraping")
        if x == "stop":
            stop_event = True

def run_value_scraper(data):
    """
    Runs the transfer value scraper program, built for multi-threading but doesn't work on my system, you can try
    :param data: The list of players to scrape along with their informations needed
    :return: None
    """
    threads = []
    num_threads = 8 # Built for multi-threading, currently not advised since the multi-threading with selenium works buggy on my system, the driver stops responding and the code doesn't finish, therefore the data gets erased without being saved
                   # Not a big performance improvement for selenium either
    chunk_size = len(data) // num_threads
    start = timeit.default_timer()

    with ThreadPoolExecutor(max_workers=num_threads) as executor:
        futures = []
        for i in range(num_threads):
            start_index = i * chunk_size

            end_index = (i + 1) * chunk_size
            futures.append(executor.submit(run_scraper,data[start_index:end_index],i))
            time.sleep(0.1) # It exceeds api rate limit for more threads than 8
        for i,future in enumerate(as_completed(futures)):
            try:
                result = future.result()

            except Exception as e:
                print(f"Error processing {i}: {e}")
        global stop_event # Built for manually stopping the scraping and saving the data, currently unavailable
        stop_event = True
    stop = timeit.default_timer() # Track the process time to calculate the total time for the whole dataset
    print(stop - start)

players = pd.read_csv("transfermarktId.csv")

def get_transfers_left(data):
    """
    This function filters out already scraped data based on 'query' values. Designed for running the code in pieces since the process takes time
    :param data: The list of all players we have enough information to scrape transfer history
    :return: Filtered list of players, only ones that are not scraped yet
    """
    df = pd.read_csv("transferData.csv")

    used_queries = pd.unique(df["Query"])  # Get unique 'query' values from the CSV
    filtered_indexes = data["Query"].isin(used_queries)# Get only not scraped players
    return data[~filtered_indexes]

players_not_scraped = get_transfers_left(players)
print(len(players_not_scraped))
run_value_scraper(players_not_scraped)
df_list = []
print("creating df")
print(result_queue.qsize())
while not result_queue.empty():
    list_ = result_queue.get()
    print(len(list_))
    df_list.extend(list_)
print("recording to csv")
pd.DataFrame(df_list).to_csv("transferData.csv", mode="a", index=False)



