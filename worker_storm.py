#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# @Author: Jairo Sanchez
# @Date:   2018-03-05 13:49:35
# @Last Modified by:   Jairo Sánchez
# @Last Modified time: 2018-03-10 01:13:17

import amqpstorm
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
    connection = amqpstorm.UriConnection(url)
    channel = connection.channel()
    channel.queue.declare(queue_name, durable=True)
    channel.basic.qos(1)  # Fetch one message at a time
    thread_name = threading.current_thread().name
    LOG.info('[%s] Waiting for tasks', thread_name)
    while True:
        message = channel.basic.get(queue=queue_name, no_ack=False)
        # If the queue is empty, task_data will contain only None
        if message is None:
            LOG.info('[%s] Nothing else to do.', thread_name)
            connection.close()
            break

        work = task.Task(message.body)
        LOG.info('[%s] Got a task %s', thread_name, work.get_id())
        rc = work.run()
        if rc != 0:
            LOG.warning('[%s] Unexpected exit code: %d', thread_name, rc)
            message.nack()
            continue

        LOG.debug('[%s] Task execution finished', thread_name)
        message.ack()
        LOG.debug('[%s] ACK sent')

        with amqpstorm.UriConnection(url) as res_conn:
            with res_conn.channel() as res_channel:
                res_channel.queue.declare(results_queue, durable=True)
                for result in work.result():
                    props = {
                        'delivery_mode': 2
                    }
                    res = amqpstorm.Message.create(res_channel, result, props)
                    res.publish(results_queue, exchange='')
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
