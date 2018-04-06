# -*- coding: utf-8 -*-
# @Author: Jairo Sanchez
# @Date:   2018-03-21 15:08:13
# @Last Modified by:   Jairo Sanchez
# @Last Modified time: 2018-03-22 15:41:43
import csv
import amqpstorm
import configparser
import argparse
import os
import json
import logging


DEFAULT_CONFIG_FILE = './disexec.config'
logging.basicConfig(level=logging.DEBUG)
LOG = logging.getLogger()


def exit_with_error(msg, code=1):
    """Exits this program with an error

    Args:
        msg (str): A message to display with the cause of the error
        code (int): The exit code to use. Default is 1
    """
    print('ERROR: {}'.format(msg))
    exit(code)


def get_results(url, queue, delete):
    """Consumes the result objects (formatted in JSON) from the specified queue

    Args:
        url (str): The URL for the queue, including user/password
        queue (str): The name of the queue
        delete (bool): Indicates if an ACK should be sent to the queue, thus
        deleting the message from the queue

    Returns:
        list: List of dictionaries
    """
    connection = amqpstorm.UriConnection(url)
    channel = connection.channel()
    channel.queue.declare(queue, durable=True)
    received = []
    while True:
        msg = channel.basic.get(queue=queue, no_ack=False)
        if msg is None:
            break
        result = json.loads(msg.body)
        if any(result):  # Avoid empty json objects
            received.append(json.loads(msg.body))
        if delete:
            msg.ack()
    connection.close()
    return received


def persist_results(results, filename):
    """Writes the content of results as a CSV file

    Args:
        results (list of dict): The results to persist
        filename (str): The path for the newly created csv file
    """
    with open(filename, 'w') as output:
        header = results[0].keys()
        writer = csv.DictWriter(output, fieldnames=header)
        writer.writeheader()
        writer.writerows(results)


def main():
    global DEFAULT_CONFIG_FILE
    desc = 'Gets the results of the experiments and persists them into a CSV \
            formatted file'
    parser = argparse.ArgumentParser(description=desc)
    parser.add_argument('-c', '--config', help='Use this configuration file',
                        type=str)
    parser.add_argument('-o', '--output', type=str, required=True,
                        help='Write the results to this file')
    parser.add_argument('-d', '--delete', default=False, action='store_true',
                        help='Deletes the data from the queue')
    args = parser.parse_args()

    config_file = DEFAULT_CONFIG_FILE
    if args.config:
        LOG.debug('Overriding default config file')
        config_file = args.config

    LOG.debug('Using configuration: %s', config_file)
    cfg = configparser.RawConfigParser()
    try:
        if not os.path.isfile(config_file):
            raise FileNotFoundError('Configuration file not found')
        cfg.read(config_file)
    except Exception as e:
        exit_with_error(e, 2)

    results = get_results(cfg.get('worker', 'queue_url'),
                          cfg.get('general', 'results_queue_name'),
                          args.delete)

    persist_results(results, args.output)


if __name__ == '__main__':
    main()
