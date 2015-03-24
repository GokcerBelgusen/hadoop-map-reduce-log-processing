#!/usr/bin/python
import sys

__author__ = 'Lucian Tuca'

import gzip
import os
import redis

from time import time
from time import gmtime, strftime
from threading import Thread
from itertools import islice

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


def get_bytes(line):
    before_status_space = -1
    start_space = -1
    last_space = -1
    line_length = len(line) - 4

    for i in range(0, line_length):
        if (line[i] == ' ') and (47 < ord(line[i + 1]) < 58) and (47 < ord(line[i + 2]) < 58) \
                and (47 < ord(line[i + 3]) < 58) and (line[i + 4] == ' '):
            before_status_space = i + 1
            break
    for i in range(before_status_space + 1, line_length):
        if line[i] == ' ':
            start_space = i
            break
    for i in range(start_space + 1, line_length):
        if line[i] == ' ':
            last_space = i
            break
    return line[start_space + 1:last_space]


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
                    _bytes = long(get_bytes(line))

                    if _ip in data:
                        data[_ip] += _bytes
                    else:
                        data[_ip] = _bytes
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


def main():
    start = time()

    normal()
    # multi_threading()

    end = time()

    ip = sys.argv[1]
    ip_total_traffic = 0

    for zip_file in input_files:
        if redis_client.hexists(zip_file, ip):
            traffic_values = long(redis_client.hget(zip_file, ip))
            ip_total_traffic += traffic_values

    print os.linesep + "====================="
    print "TRAFIC: %s = %s." % (ip, ip_total_traffic)
    print "TOTAL TIME: %s" % str(end - start)
    print "=====================" + os.linesep

    total_traffic = 0
    for zip_file in input_files:
        ip_dict = redis_client.hgetall(zip_file)
        for ip in ip_dict:
            total_traffic += long(ip_dict[ip])
    print "TOTAL TRAFFIC: ", total_traffic

if __name__ == "__main__":
    main()

