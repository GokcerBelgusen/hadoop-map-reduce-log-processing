#!/usr/bin/python

"""
Requirments:
    1. Python 2.7.
    2. Redis (server already started).
    3. 'data' folder on the same level with the 'main.py' script which contains the logs.
"""

__author__ = 'Lucian Tuca'

from time import time
from time import gmtime, strftime
from threading import Thread
from itertools import islice
import gzip
import os

import redis


redis_client = redis.StrictRedis(host='localhost', port=6379, db=0)
redis_client.flushall()

input_folder = 'data'
input_files = []
data_files = os.listdir(input_folder)
for input_file in data_files:
    if '.DS_Store' not in input_file:
        file_path = os.path.join(input_folder, input_file)
        input_files.append(file_path)


def get_ip(line):
    for i in range(7, 16):
        if line[i] == ' ':
            return line[0:i]


def get_bytes_and_status(line):
    before_status_space = -1
    start_space = -1
    last_space = -1
    line_length = len(line) - 4
    status = ""

    for i in range(0, line_length):
        if (line[i] == ' ') and (47 < ord(line[i + 1]) < 58) and (47 < ord(line[i + 2]) < 58) \
                and (47 < ord(line[i + 3]) < 58) and (line[i + 4] == ' '):
            before_status_space = i + 1
            status = line[before_status_space:before_status_space + 3]
            break

    for i in range(before_status_space + 1, line_length):
        if line[i] == ' ':
            start_space = i
            break
    for i in range(start_space + 1, line_length):
        if line[i] == ' ':
            last_space = i
            break
    return line[start_space + 1:last_space], status


def process_zip(zip_path):
    start_f = time()
    start_gmt = gmtime()

    end = long(50)

    if not redis_client.exists(zip_path):
        print "====================="
        print "PROCESSING: %s." % zip_path

        data = {}
        lines = ['1']
        with gzip.open(zip_path, "rb") as process_file:

            while len(lines) > 0:
                lines = list(islice(process_file, end))
                for line in lines:
                    _ip = get_ip(line)
                    _bytes, _status = get_bytes_and_status(line)
                    _bytes = long(_bytes)

                    if _ip in data:
                        data[_ip] += [{"status": _status, "bytes": _bytes}]
                    else:
                        data[_ip] = [{"status": _status, "bytes": _bytes}]
                end += 50

        redis_client.hmset(zip_path, data)
        print "DONE: %s." % zip_path
        print "---------------------"
    else:
        print "ALREADY DONE : %s." % zip_path

    end_f = time()

    print "STARTED @ %s" % strftime("%Y-%m-%d %H:%M:%S", start_gmt)
    print "ENDED @ %s" % strftime("%Y-%m-%d %H:%M:%S", gmtime())
    print "TIME FOR %s : %s" % (zip_path, str(end_f - start_f))
    print "=====================" + os.linesep


def normal():
    for zip_file in input_files:
        process_zip(zip_file)


def multi_threading():
    threads = []

    for zip_file in input_files:
        threads.append(Thread(target=process_zip, args=(zip_file,)))

    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()


def req_1(ip):
    """
    # REQ 1
    Searching in a set of Apache logs, compute how much bandwidth an IP has used.
    """
    ip_total_traffic = 0

    for zip_file in input_files:
        if redis_client.hexists(zip_file, ip):
            ip_traffic_data = eval(redis_client.hget(zip_file, ip))
            for traffic_data in ip_traffic_data:
                ip_total_traffic += traffic_data["bytes"]

    print os.linesep + "====================="
    print "TRAFIC: %s = %s." % (ip, ip_total_traffic)
    print "=====================" + os.linesep

    return ip_total_traffic


def req_2(ip):
    """
    # REQ 2
    Given an IP, break down how much bandwidth has been used by status code (200, 404, 500, etc).
    Give the results in bytes as well as in percentages of the total.
    """
    results_data = {}
    ip_total_traffic = req_1(ip)

    for zip_file in input_files:
        if redis_client.hexists(zip_file, ip):
            ip_traffic_data = eval(redis_client.hget(zip_file, ip))
            for traffic_data in ip_traffic_data:

                _status = traffic_data["status"]
                _bytes = traffic_data["bytes"]

                if _status not in results_data:
                    results_data[_status] = _bytes
                else:
                    results_data[_status] += _bytes

    print "Total traffic for IP: %s = %s" % (ip, ip_total_traffic)
    for status_code in results_data:
        traffic = results_data[status_code]
        percent = (traffic * 100.0) / float(ip_total_traffic)
        print "\t Status %s : %s%% = %s bytes" % (status_code, percent, traffic)


def req_3():
    """
    Without an IP given, print the global bandwidth statistics,
    broken down by status code, in bytes as well as in percentages.
    """
    results_data = {"total_traffic": 0}

    total_traffic = 0
    for zip_file in input_files:
        ip_dict = redis_client.hgetall(zip_file)
        for ip in ip_dict:
            ip_traffic_data = eval(ip_dict[ip])

            for ip_data in ip_traffic_data:
                _status = ip_data["status"]
                _bytes = ip_data["bytes"]
                total_traffic += _bytes

                if _status not in results_data:
                    results_data[_status] = _bytes
                else:
                    results_data[_status] += _bytes

    results_data["total_traffic"] = total_traffic

    for status_code in results_data:
        traffic = results_data[status_code]
        percent = (traffic * 100.0) / float(results_data["total_traffic"])
        print "\t Status %s : %s%% = %s bytes" % (status_code, percent, traffic)


def main():
    start = time()

    normal()
    # multi_threading()

    ip = '178.154.179.250'

    req_1(ip)
    req_2(ip)
    req_3()

    end = time()
    print "Total time: %s seconds." % (end - start)


if __name__ == "__main__":
    main()