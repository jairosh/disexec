#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# @Author: Jairo Sanchez
# @Date:   2018-03-05 13:49:35
# @Last Modified by:   Jairo Sánchez
# @Last Modified time: 2018-04-21 14:33:52

import amqpstorm
import task
import argparse
import configparser
import os
import threading
import logging


DEFAULT_CONFIG_FILE = './disexec.config'
DEFAULT_NBR_OF_THREADS = 4
LOG_FILE = './worker.log'
LOG_FORMAT = '%(asctime)s %(name)-12s %(threadName)s %(levelname)-8s %(message)s'

log = logging.getLogger()
logging.basicConfig(filename=LOG_FILE, level=logging.DEBUG, format=LOG_FORMAT)

console = logging.StreamHandler()
console.setLevel(logging.DEBUG)
formatter = logging.Formatter(LOG_FORMAT)
console.setFormatter(formatter)
logging.getLogger('').addHandler(console)

logging.getLogger('amqpstorm').setLevel(logging.INFO)


def worker_thread(url, queue_name, results_queue):
    """Worker thread, for each instance
    """
    global log
    empty_queue = False
    while not empty_queue:
        connection = amqpstorm.UriConnection(url)
        channel = connection.channel(rpc_timeout=120)
        channel.queue.declare(queue_name, durable=True)
        channel.basic.qos(1)  # Fetch one message at a time
        log.info('Waiting for tasks')
        while True:
            message = channel.basic.get(queue=queue_name, no_ack=False)
            # If the queue is empty, task_data will contain only None
            if message is None:
                log.info('Nothing else to do.')
                connection.close()
                empty_queue = True
                break

            work = task.Task(message.body)
            log.info('Got a task %s', work.get_id())
            rc = work.run()
            if rc != 0:
                log.warning('Unexpected exit code: %d', rc)
                stdout = work.get_stdout()
                stderr = work.get_stderr()
                if stdout is not None:
                    log.error('STDOUT: %s', stdout)
                if stderr is not None:
                    log.error('STDERR: %s', stderr)
                message.nack()
                continue

            log.debug('Task execution finished')
            message.ack()

            with amqpstorm.UriConnection(url) as res_conn:
                with res_conn.channel() as res_channel:
                    res_channel.queue.declare(results_queue, durable=True)
                    for result in work.result():
                        props = {
                            'delivery_mode': 2
                        }
                        res = amqpstorm.Message.create(res_channel,
                                                       result,
                                                       props)
                        res.publish(results_queue, exchange='')
            log.debug('Task and result processing completed')
            res_conn.close()

        connection.close()

    log.info('Thread exiting. (empty queue? %s)',
             repr(empty_queue))


def exit_with_error(msg, code):
    print(msg)
    exit(code)


def main():
    global log
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

    log.debug('Reading configuration file at %s', config_file)
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
