import time
import math
import hashlib
import bootstrap_init
from global_state import global_instance
from pandas import DataFrame

from util.utility import time_monitoring_task
from util.utility import estimate_current_job_time # EMA time
from util.utility import divide_pages_into_three_parts

import threading
from concurrent.futures import ThreadPoolExecutor

from bs4 import BeautifulSoup

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.support import expected_conditions as EC

def coreWorkday(df: DataFrame) -> None:
    """
    DOC STRING
    """
    logger = bootstrap_init.logger

    if (global_instance.get_data("chrome_options") == None):
        logger.error("Chrome Options for Selenium are not set! Aborting!")
        raise Exception("Chrome Options for Selenium are not set! Aborting!")

    logger.info("Executing Workday Runner...")
    logger.debug("Printing df...")
    print(df)

    job_loss_rate_arr = []
    historical_EMA_Predictions = [0] # Initialize with 200 Seconds of EMA
    actual_completion_times = [200] # Initalize with actual time

    workday_jobs = {}
    workday_jobs["Job_Title"] = []
    workday_jobs["Job_Location"] = []
    workday_jobs["Job_Posted_Time"] = []
    workday_jobs["Job_Link"] = []
    workday_jobs["Job_ID"] = []
    workday_jobs["Job_Meta"] = []

    global_instance.modify_data("worday_dict", workday_jobs)

    return

def workday_job_scraper_multithread(url: str, job_loss_rate_arr: list, historical_EMA_Predictions: list[int], actual_completion_times: list[int], logger) -> bool:
    """
    DOC STRING
    """
    chrome_options = global_instance.get_data("chrome_options")

    start_time = time.time()
    estimated_time = estimate_current_job_time(historical_EMA_Predictions, actual_completion_times)
    ema_with_constant = estimated_time + 300
    
    try:
        driver = webdriver.Chrome(options=chrome_options)
        driver.set_page_load_timeout(30)
        driver.get(url)
        time.sleep(3)
    
        page_source = driver.page_source
        soup = BeautifulSoup(page_source, "html.parser")
    
        driver.quit()
    except Exception as e:
        print(f"Could not get total number of jobs!")
        print(f"Error occcured at for {url} : {e}")
        return False
    
    p_element = soup.find('p', {'data-automation-id': 'jobOutOfText'})
    total_jobs = -1
    
    if (p_element):
        total_jobs = int(p_element.get_text().split()[4])
    else:
        return False

    total_pages = total_jobs // 20

    print(f"*** FOR {url} ***")
    print(f"Total number of jobs is: {total_jobs}")
    print(f"Total number of pages is: {total_pages}")
    print(f"Estimated time for completion of longest running thread: {estimated_time}")

    if (total_pages < 3):
        workday_job_scraper(url, 1, total_pages, ema_with_constant, logger)
    else:
        partition = divide_pages_into_three_parts(total_pages)
    
        logger.info(f"Partitioning pages scheme per thread: {partition}")
        logger.debug("Executing Threading!")
        
        with ThreadPoolExecutor(max_workers=3) as executor:
            jobs_to_scrape = [
                (f"{url}", partition[0][0], partition[0][1], ema_with_constant),
                (f"{url}", partition[1][0], partition[1][1], ema_with_constant),
                (f"{url}", partition[2][0], partition[2][1], ema_with_constant)
            ]
            futures = [executor.submit(workday_job_scraper, url, start, finish, duration, logger) for url, start, finish, duration in jobs_to_scrape]

        overall_jobs_lost = 0
        for future in futures:
            overall_jobs_lost += future.result()
        percentage = (overall_jobs_lost / total_jobs) * 100
        job_loss_rate_arr.append(percentage)

        end_time = time.time()
        elapsed_time = end_time - start_time
        actual_completion_times.append(elapsed_time)
        historical_EMA_Predictions.append(estimated_time)
        
        print("\n --- Scrape Statistics --- ")
        print(f"Execution time: {elapsed_time} seconds")
        print(f"Jobs Lost this Workday Link: {overall_jobs_lost}")
        print(f"Jobs Loss: {percentage}%")
        print(f"Average Job Loss Rate: {math.trunc((sum(job_loss_rate_arr) / len(job_loss_rate_arr)) * 100) / 100}% \n\n")
    
    return True

def workday_job_scraper(url, start, finish, duration, logger):
    """
    DOC STRING
    """
    workday_jobs = global_instance.get_data("workday_dict")

    page = start
    prev_page = start
    stop_event = threading.Event()
    current_thread_name = threading.current_thread().name
    executor = ThreadPoolExecutor()
    chrome_options = global_instance.get_data("chrome_options")
    
    terms = [
        "software", "developer", "data", ".Net", "C#", "C", "C++",
        "full stack", "backend", "front end", "frontend", "backend", 
        "back-end", "back end", "systems", "DevOps", "site reliability"
    ]
    
    # Initialize the thread timer
    try:
        executor.submit(time_monitoring_task, duration, stop_event)
    except Exception as e:
        print(f"[{current_thread_name}] Could not spawn time thread! Exiting!")
        print(f"{e}")
        total_lost_jobs = (((finish - start) + 1) * 20)
        return total_lost_jobs

    # Then start scraping
    try:
        retries = 100
        hash_set = set()
        
        driver = webdriver.Chrome(options=chrome_options)
        driver.set_page_load_timeout(30)
        driver.get(url)
        time.sleep(10)
            
        walk_start = 1
        while(walk_start != start and not stop_event.is_set()):
            walk_start += 1 
            driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            
            try:
                WebDriverWait(driver, 100).until(EC.element_to_be_clickable((By.XPATH, f"//button[@data-uxi-widget-type='paginationPageButton' and text()='{walk_start}']")))
            except TimeoutException:
                print(f"Could not find the pagination button for page {walk_start} within the specified time.")
                
            buttonA = driver.find_element(By.XPATH, f"//button[@data-uxi-widget-type='paginationPageButton' and text()='{walk_start}']")
            buttonA.click()

        if (stop_event.is_set()):
            logger.error("Thread Timed out during Walk!")
            total_lost_jobs = (((finish - start) + 1) * 20)
            driver.quit()
            return total_lost_jobs
        else:  
            logger.debug(f"[{current_thread_name}] Finished Walk!")

        retry_cnt = 0
        total_lost_jobs = 0
        while(page <= finish and retry_cnt != 10 and not stop_event.is_set()):
            WebDriverWait(driver, retries).until(EC.presence_of_all_elements_located((By.TAG_NAME, "li")))
            page_source = driver.page_source
            soup = BeautifulSoup(page_source, "html.parser")
            
            li_elements = soup.find_all('li')

            """Core Scraping Logic"""
            skipped_jobs = 0
            duplicate_cnt = 0
            for li in li_elements:
                job_title_raw = li.find('a', {'data-automation-id': 'jobTitle'})
                location_raw = li.find('div', {'data-automation-id': 'locations'})
                postedTime_raw = li.find('div', {'data-automation-id': 'postedOn'})
                jobID_raw = li.find('ul', {'data-automation-id': 'subtitle'})
                
                if (job_title_raw and location_raw and postedTime_raw and jobID_raw):
                    # Job Title
                    job_title = job_title_raw.get_text()
                    
                    # Location
                    location = None
                    dd_elements_loc = location_raw.find_all('dd')
                    for dd in dd_elements_loc:
                        location = dd.get_text()
    
                    # Time
                    postedTime = None
                    dd_elements_time = postedTime_raw.find_all('dd')
                    for dd in dd_elements_time:
                        postedTime = dd.get_text()
    
                    # Job Link
                    job_link = (url.split('.com')[0] + '.com' if '.com' in url else url) + job_title_raw['href']

                    # Job ID
                    jobID = None
                    li_elements_id = jobID_raw.find_all('li')
                    readableID = ""
                    for li in li_elements_id:
                        readableID += li.get_text()
                    jobID = hashlib.sha256(readableID.encode())
                        
                    term_found = False
                    if (jobID not in hash_set):
                        for term in terms:
                            if term in job_title.lower():
                                term_found = True
                        if (term_found):
                            list_lock_A = threading.Lock()
                            with list_lock_A:
                                workday_jobs["Job_Title"].append(job_title)
                                workday_jobs["Job_Location"].append(location)
                                workday_jobs["Job_Posted_Time"].append(postedTime)
                                workday_jobs["Job_Link"].append(job_link)
                                workday_jobs["Job_ID"].append(jobID)
                                workday_jobs["Job_Meta"].append(readableID)
                        hash_set.add(jobID)
                    else:
                        duplicate_cnt += 1
                elif ((job_title_raw == None) ^ (location_raw == None) ^ (postedTime_raw == None) ^ (jobID_raw == None)):
                    skipped_jobs += 1

            if (duplicate_cnt <= 5): # Having some duplicates is fine (if possible), but a whole page is unlikely
                total_lost_jobs += skipped_jobs
                total_lost_jobs += duplicate_cnt

                prev_page = page
                page += 1
                if (page > finish):
                    break
                    
                driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                WebDriverWait(driver, retries).until(EC.element_to_be_clickable((By.XPATH, f"//button[@data-uxi-widget-type='paginationPageButton' and text()='{page}']")))
                buttonB = driver.find_element(By.XPATH, f"//button[@data-uxi-widget-type='paginationPageButton' and text()='{page}']")
                buttonB.click()

                
                WebDriverWait(driver, retries).until(EC.presence_of_all_elements_located((By.TAG_NAME, "li")))
                WebDriverWait(driver, retries).until(EC.presence_of_all_elements_located((By.TAG_NAME, "button")))
            elif (prev_page == page):
                retry_cnt += 1
                print(f"[{current_thread_name}] Current Page Contained too many Duplicates! ({duplicate_cnt}) Retrying...")
                time.sleep(3)
                
        if (retry_cnt == 10):
            logger.error("Hit Max Retry Count!")
            total_lost_jobs = (((finish - start) + 1) * 20)
        elif (stop_event.is_set()):
            logger.error("Thread Timed out!")
            total_lost_jobs = (((finish - start) + 1) * 20)
        
        logger.info(f"[{current_thread_name}] Total Jobs Lost: {total_lost_jobs}")
        driver.quit()
        stop_event.set()
        executor.shutdown(wait=True)
        
        return total_lost_jobs
    except Exception as e:
        current_thread_name = threading.current_thread().name
        total_lost_jobs = (((finish - start) + 1) * 20)
        print(f"[ERROR] {current_thread_name} ran into an error!")
        print(f"[ERROR] on page {page}!")
        logger.exception(f"Error occcured for {url}")
        return total_lost_jobs