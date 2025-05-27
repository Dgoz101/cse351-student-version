"""
Course    : CSE 351
Assignment: 04
Student   : David Gosney

Instructions:
    - review instructions in the course

In order to retrieve a weather record from the server, use the URL:
    f"{TOP_API_URL}/record/{name}/{recno}"
where:
    name: name of the city
    recno: record number starting from 0
"""

import threading
import queue
from common import *
from cse351 import Log


THREADS = 200               
WORKERS = 10                
RECORDS_TO_RETRIEVE = 5000  # Don't change


def retrieve_weather_data(task_q, result_q):
    """
    Fetch threads: pull (city, recno) from task_q, call server,
    then push (city, date, temp) into result_q. Stops on None sentinel.
    """
    while True:
        task = task_q.get()
        if task is None:
            # Notify queue and exit
            task_q.task_done()
            break
        city, recno = task
        data = get_data_from_server(f"{TOP_API_URL}/record/{city}/{recno}")
        if data:
            result_q.put((data['city'], data['date'], data['temp']))
        task_q.task_done()


class Worker(threading.Thread):
    def __init__(self, result_q, noaa):
        super().__init__()
        self.result_q = result_q
        self.noaa = noaa
        self.start()

    def run(self):
        while True:
            item = self.result_q.get()
            if item is None:
                self.result_q.task_done()
                break
            city, date, temp = item
            self.noaa.add_record(city, date, temp)
            self.result_q.task_done()


class NOAA:
    def __init__(self):
        self.lock = threading.Lock()
        # sum and count for each city
        self.totals = {name: {'sum': 0.0, 'count': 0} for name in CITIES}

    def add_record(self, city, date, temp):
        with self.lock:
            self.totals[city]['sum'] += temp
            self.totals[city]['count'] += 1

    def get_temp_details(self, city):
        with self.lock:
            total = self.totals[city]['sum']
            count = self.totals[city]['count']
        return (total / count) if count > 0 else 0.0


def verify_noaa_results(noaa):
    expected = {
        'sandiego': 14.5004,
        'philadelphia': 14.865,
        'san_antonio': 14.638,
        'san_jose': 14.5756,
        'new_york': 14.6472,
        'houston': 14.591,
        'dallas': 14.835,
        'chicago': 14.6584,
        'los_angeles': 15.2346,
        'phoenix': 12.4404,
    }
    print()
    print('NOAA Results: Verifying Results')
    print('===================================')
    for name in CITIES:
        avg = noaa.get_temp_details(name)
        status = 'PASSED' if abs(avg - expected[name]) < 1e-5 else f'FAILED  Expected {expected[name]}'
        print(f'{name:>15}: {avg:<10} {status}')
    print('===================================')


def main():
    log = Log(show_terminal=True, filename_log='assignment.log')
    log.start_timer()

    # Initialize NOAA aggregator
    noaa = NOAA()

    # Start the server session
    get_data_from_server(f"{TOP_API_URL}/start")

    # Retrieve city details
    print('Retrieving city details')
    print(f"{'City':>15}: Records")
    print('===================================')
    for city in CITIES:
        info = get_data_from_server(f"{TOP_API_URL}/city/{city}")
        records = info['records']
        print(f"{city:>15}: Records = {records:,}")
    print('===================================')

    # Create bounded queues
    task_q = queue.Queue(maxsize=10)
    result_q = queue.Queue(maxsize=10)

    # Launch worker threads
    workers = [Worker(result_q, noaa) for _ in range(WORKERS)]

    # Launch fetch threads
    fetchers = []
    for _ in range(THREADS):
        t = threading.Thread(target=retrieve_weather_data, args=(task_q, result_q))
        t.start()
        fetchers.append(t)

    # Enqueue all fetch tasks
    for city in CITIES:
        for recno in range(RECORDS_TO_RETRIEVE):
            task_q.put((city, recno))

    # Send sentinel None to fetch threads
    for _ in range(THREADS):
        task_q.put(None)

    # Wait for all fetchers to finish
    for t in fetchers:
        t.join()

    # Notify workers with sentinels
    for _ in range(WORKERS):
        result_q.put(None)

    # Wait for workers to finish processing
    for w in workers:
        w.join()

    # Shut down server
    end_info = get_data_from_server(f"{TOP_API_URL}/end")
    print(end_info)

    # Verify correctness
    verify_noaa_results(noaa)

    log.stop_timer('Run time: ')


if __name__ == '__main__':
    main()
