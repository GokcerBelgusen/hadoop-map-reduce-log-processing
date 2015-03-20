#!/usr/bin/python
import sys

__author__ = 'Lucian Tuca'

import gzip
import os
import redis

from re import findall
from time import time
from time import gmtime, strftime
from threading import Thread

redis_client = redis.StrictRedis(host='localhost', port=6379, db=0)


def get_ip(line):
    for i in range(0, 16):
        if line[i] == ' ':
            return line[0:i]


def get_bytes(line):
    regexp = '\d{3}\s(\d*)\s'
    return findall(regexp, line)[0]


def update_in_db(_ip, _bytes):
    val = redis_client.get(_ip)

    if val is None:
        val = 0

    val = long(val)
    redis_client.set(_ip, val + long(_bytes))


def process_line(line):
    _ip = get_ip(line)
    _bytes = get_bytes(line)
    # update_in_db(_ip, _bytes)
    redis_client.lpush(_ip, _bytes)


def process_zip(zip_path):
    start_f = time()
    start_gmt = gmtime()

    if not redis_client.exists(zip_path):
        print "PROCESSING: %s." % zip_path
        process_file = gzip.GzipFile(zip_path, 'rb')
        for line in process_file:
            process_line(line)
        process_file.close()
        redis_client.set(zip_path, True)
        print "DONE: %s." % zip_path
    else:
        print "ALREADY DONE : %s." % zip_path

    end_f = time()
    print os.linesep + "=====================" + os.linesep
    print "STARTED @ %s" % strftime("%Y-%m-%d %H:%M:%S", start_gmt)
    print "ENDED @ %s" % strftime("%Y-%m-%d %H:%M:%S", gmtime())
    print "TIME FOR %s : %s" % (zip_path, str(end_f - start_f))
    print os.linesep + "=====================" + os.linesep


def normal():
    input_folder = 'data'
    input_files = os.listdir(input_folder)

    for input_file in input_files:
        file_path = os.path.join(input_folder, input_file)
        process_zip(file_path)


def multi_threading():
    input_folder = 'data'
    input_files = os.listdir(input_folder)

    threads = []

    for input_file in input_files:
        file_path = os.path.join(input_folder, input_file)
        threads.append(Thread(target=process_zip, args=(file_path,)))

    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()


def main():
    redis_client.flushall()
    start = time()

    #normal()
    multi_threading()

    end = time()

    print "TOTAL TIME: %s" % str(end - start)

    ip = sys.argv[1]
    if redis_client.exists(ip):
        # traffic = redis_client.get(ip)
        # print "Traffic for IP %s = %s bytes" % (ip, str(traffic))

        traffic_values = redis_client.lrange(ip, 0, -1)
        total_traffic = 0
        for number in traffic_values:
            total_traffic += long(number)
        print total_traffic

if __name__ == "__main__":
    main()

