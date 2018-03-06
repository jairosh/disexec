#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# @Author: Jairo Sanchez
# @Date:   2018-03-05 13:49:35
# @Last Modified by:   Jairo Sanchez
# @Last Modified time: 2018-03-05 17:31:00

import pika
import task
import argparse
import configparser
import os
import subprocess
import threading
import time


DEFAULT_CONFIG_FILE = './disexec.config'
DEFAULT_NBR_OF_THREADS = 4


def worker_thread(url, queue_name):
    """Worker thread, for each instance o
    """
    conn_params = pika.URLParameters(url)
    connection = pika.BlockingConnection(conn_params)
    channel = connection.channel()
    channel.queue_declare(queue=queue_name)
    while True:
        task_data = channel.basic_get(queue=queue_name, no_ack=False)
        # If the queue is empty, task_data will contain only None
        if all([x is None for x in task_data]):
            connection.close()
            break

        work = task.Task(task_data[2])
        work.run()
        print(work)
        # completed = subprocess.run([ONE_EXECUTABLE, '-b', '50', tmp_file],
        #                           cwd=os.path.dirname(ONE_EXECUTABLE))
        channel.basic_ack(delivery_tag=task_data[0].delivery_tag)


def exit_with_error(msg, code):
    print(msg)
    exit(code)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('-c', '--config', help='Configuration file', type=str)
    args = parser.parse_args()

    config_file = DEFAULT_CONFIG_FILE
    if args.config:
        config_file = args.config

    cfg = configparser.RawConfigParser()
    try:
        if not os.path.isfile(config_file):
            raise FileNotFoundError('Unable to locate the config file')
        cfg.read(config_file)
    except Exception as e:
        exit_with_error(e)

    threads = []
    workers = cfg.get('worker', 'cores')
    args = (cfg.get('worker', 'queue_url'), cfg.get('general', 'queue_name'))
    for i in range(int(workers)):
        thread = threading.Thread(target=worker_thread, args=args)
        thread.setName('worker-{}'.format(i))
        thread.start()
        threads.append(thread)

    for t in threads:
        t.join()


if __name__ == '__main__':
    main()
