#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# @Author: Jairo Sanchez
# @Date:   2018-03-01 13:52:34
# @Last Modified by:   Jairo Sanchez
# @Last Modified time: 2018-04-11 18:52:51

import amqpstorm
import argparse
import configparser
import os
import sys
import json
import csv


DEFAULT_CONFIG_FILE = './disexec.config'


class TaskCreator(object):
    """Defines an abstract class to create the tasks to be posted at the queue
    """

    def __init__(self, csv_file, config):
        pass

    def create_tasks(self):
        """Any implementation of this method has to create a list of JSON
        objects representing the tasks"""
        raise NotImplementedError('Implement this method in inherited class')


class ParamsInExternalFileCreator(TaskCreator):

    """Defines a task creator that will populate the ${external_data} var
        with the data extracted from a CSV file. The CSV format is: first row
        has the headers/variable names, each has the variables for a Task and
        it'll be created a pair of the form
                    $header=$cell_value
        and then concatenated with new lines
                    header_1=value1\n$header_2=...
    """

    def __init__(self, filepath, config):
        """Constructor

        Args:
            filepath (str): The path to the CSV file
            config (RawConfigParser): The existant configuration parser, used
            to embed the additional parameters to each Task
        """
        self._csv = filepath
        self._config = config

    def create_tasks(self):
        """Creates the tasks

        Returns:
            list: List of JSON formatted strings
        """
        json_tasks = []
        names, values = self.read_csv_parameters(self._csv)
        index = 0
        for datatask in values:
            task = {}
            # A unique id, it'll be used as a filename (if external_data)
            task['id'] = index
            # The contents of this var will be written to a file and passed
            # to the command
            sim_config = ''
            for idx in range(len(datatask)):
                sim_config = sim_config + '{0}={1}\n'.format(names[idx],
                                                             datatask[idx])
            task['external_data'] = sim_config
            # Folder where all the external_data files will be written, if not
            # present, then it will use a temp folder
            task['external_data_folder'] = self._config.get('task',
                                                            'external_folder')
            # The command to execute in each worker, be aware of the $PATH
            # in all of the workers
            task['command'] = self._config.get('task', 'command')
            # Extra arguments or flags in the command
            task['arguments'] = self._config.get('task', 'arguments')

            json_tasks.append(json.dumps(task))
            index = index + 1

        return json_tasks

    def read_csv_parameters(csvfile):
        """Parses a csv file into two lists, one with the parameter names and
        another with all the values that takes per run

        Args:
            csvfile (str): The path to the CSV file to be parsed

        Returns:
            tuple: List with the names of columns in the first field, followed
            by a list of lists for each row
        """
        col_names = []
        fields = []
        values = []
        with open(csvfile, 'r') as the_file:
            param_reader = csv.reader(the_file)
            for row in param_reader:
                fields.append(row)

        col_names = fields[:1][0]
        values = fields[1:]
        return col_names, values


def exit_with_error(why, code):
    """Terminates execution of this program

    Args:
        msg (str): Message to display before crashing
        code (int): The status code to return on the shell
    """
    print(why)
    exit(code)


def start(config):
    """Creates the TaskCreator object specified in the configuration file
    calls it and push the tasks to the Message queue

    Args:
        config (RawConfigParser): The configuration reader
    """
    task_creator = config.get('coordinator', 'taskcreator')
    csv_file = config.get('coordinator', 'csvfile')
    url = config.get('coordinator', 'queue_url')
    queue_name = config.get('general', 'queue_name')

    creator_class = getattr(sys.modules[__name__], task_creator)
    creator = creator_class(csv_file, config)
    tasks = creator.create_tasks()
    connection = amqpstorm.UriConnection(url)
    channel = connection.channel(rpc_timeout=120)
    channel.queue.declare(queue_name, durable=True)
    for task in tasks:
        print('Pushing into queue:\n{0}'.format(task))
        channel.basic.publish(task, queue_name, exchange='',
                              properties={'delivery_mode': 2})
    connection.close()


def main():
    """Main function

    Raises:
        FileNotFoundError: In case there is no configuration file
    """
    parser = argparse.ArgumentParser()
    parser.add_argument('-c', '--config', help='Load the configuration file',
                        type=str)
    args = parser.parse_args()
    config_file = DEFAULT_CONFIG_FILE
    if args.config:
        config_file = args.config

    cfg = configparser.RawConfigParser()
    try:
        if not os.path.isfile(config_file):
            raise FileNotFoundError('Invalid config file')

        cfg.read(config_file)
    except Exception as e:
        exit_with_error(e, 1)

    start(cfg)


if __name__ == '__main__':
    main()
