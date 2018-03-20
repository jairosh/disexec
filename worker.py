#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# @Author: Jairo Sanchez
# @Date:   2018-03-05 13:49:35
# @Last Modified by:   Jairo Sánchez
# @Last Modified time: 2018-03-10 00:25:01

import pika
import task
import argparse
import configparser
import os
import threading
import logging


DEFAULT_CONFIG_FILE = './disexec.config'
DEFAULT_NBR_OF_THREADS = 4


logging.basicConfig(level=logging.DEBUG)
LOG = logging.getLogger()


def worker_thread(url, queue_name, results_queue):
    """Worker thread, for each instance o
    """
    conn_params = pika.URLParameters(url)
    connection = pika.BlockingConnection(conn_params)
    channel = connection.channel()
    channel.queue_declare(queue=queue_name, durable=True)
    channel.basic_qos(prefetch_count=1)
    thread_name = threading.current_thread().name
    LOG.info('[%s] Waiting for tasks', thread_name)
    while True:
        task_data = channel.basic_get(queue=queue_name, no_ack=False)
        # If the queue is empty, task_data will contain only None
        if all([x is None for x in task_data]):
            LOG.info('[%s] Nothing else to do.', thread_name)
            connection.close()
            break

        work = task.Task(task_data[2])
        LOG.info('[%s] Got a task %s', thread_name, work.get_id())
        rc = work.run()
        if rc != 0:
            LOG.warning('[%s] Unexpected exit code: %d', thread_name, rc)
            channel.basic_nack(delivery_tag=task_data[0].delivery_tag)
            continue

        LOG.debug('[%s] Task execution finished', thread_name)
        channel.basic_ack(delivery_tag=task_data[0].delivery_tag)
        LOG.debug('[%s] ACK sent')

        res_conn = pika.BlockingConnection(conn_params)
        res_channel = res_conn.channel()
        res_channel.queue_declare(results_queue, durable=True)
        prop = pika.BasicProperties(delivery_mode=2)
        for result in work.result():
            res_channel.basic_publish(exchange='',
                                      routing_key=results_queue,
                                      body=result,
                                      properties=prop)
        LOG.debug('[%s] Task and result processing completed')
        res_conn.close()

    connection.close()


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
    args = (cfg.get('worker', 'queue_url'), cfg.get('general', 'queue_name'),
            cfg.get('general', 'results_queue_name'))
    for i in range(int(workers)):
        thread = threading.Thread(target=worker_thread, args=args)
        thread.setName('worker-{}'.format(i))
        thread.start()
        threads.append(thread)

    for t in threads:
        t.join()


if __name__ == '__main__':
    main()
