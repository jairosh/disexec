#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# @Author: Jairo Sanchez
# @Date:   2018-03-01 13:52:34
# @Last Modified by:   Jairo Sanchez
# @Last Modified time: 2018-03-01 18:31:00

import pika
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

    def __init__(self):
        pass

    def create_tasks(self):
        """Any implementation of this method has to create a list of JSON
        objects representing the tasks"""
        raise NotImplementedError('Implement this method in inherited class')


class ParamsInExternalFileCreator(TaskCreator):
    def __init__(self, filepath):
        self._csv = filepath

    def create_tasks(self):
        json_tasks = []
        names, values = read_csv_parameters(self._csv)
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
                print('Task Added: {0}'.format([index, sim_config]))
            task['external_data'] = sim_config
            # Folder where all the external_data files will be written, if not
            # present, then it will use a temp folder
            task['external_data_folder'] = ''
            # The command to execute in each worker, be aware of the $PATH
            # in all of the workers
            task['command'] = ''
            # Extra arguments or flags in the command
            task['arguments'] = ''

            json_tasks.append(json.dumps(task))
            index = index + 1

        return json_tasks


def read_csv_parameters(csvfile):
    """ Parses a csv file into two lists, one with the parameter names and
        another with all the values that takes per run
    """
    col_names = []
    fields = []
    values = []
    with open(csvfile, 'r') as the_file:
        param_reader = csv.reader(the_file)
        for row in param_reader:
            fields.append(row)

    col_names = fields[:1]
    values = fields[1:]
    return col_names, values


def exit_with_error(msg, code):
    print(msg)
    exit(code)


def start(task_creator, url, csv_file):
    creator_class = getattr(sys.modules[__name__], task_creator)
    creator = creator_class(csv_file)
    tasks = creator.create_tasks()
    print(tasks)


def main():
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

    start(cfg.get('coordinator', 'taskcreator'),
          cfg.get('general', 'queue_url'),
          cfg.get('coordinator', 'csvfile'))


if __name__ == '__main__':
    main()
