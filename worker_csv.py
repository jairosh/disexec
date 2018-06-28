#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Summary

Attributes:
    console (TYPE): Description
    DEFAULT_CONFIG_FILE (str): Description
    DEFAULT_NBR_OF_THREADS (int): Description
    formatter (TYPE): Description
    IDS_DONE (list): Description
    JOBS_DONE (dict): Description
    log (TYPE): Description
    LOG_FILE (str): Description
    LOG_FORMAT (str): Description
    QUEUE (TYPE): Description
"""
# @Author: Jairo Sánchez
# @Date:   2018-06-27 01:45:02
# @Last Modified by:   Jairo Sánchez
# @Last Modified time: 2018-06-28 13:16:05

import csv
import os
import argparse
import configparser
import threading
import logging
import queue
import json
import sys
# Local files
import task

DEFAULT_CONFIG_FILE = './disexec.config'
DEFAULT_NBR_OF_THREADS = 6
LOG_FILE = './worker_csv.log'
LOG_FORMAT = '%(asctime)s %(name)-12s %(threadName)s %(levelname)-8s %(message)s'
IDS_DONE = []
JOBS_DONE = {}
QUEUE = queue.Queue()

log = logging.getLogger()
logging.basicConfig(filename=LOG_FILE, level=logging.DEBUG, format=LOG_FORMAT)

console = logging.StreamHandler()
console.setLevel(logging.DEBUG)
formatter = logging.Formatter(LOG_FORMAT)
console.setFormatter(formatter)
logging.getLogger('').addHandler(console)


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

    def read_csv_parameters(self, csvfile):
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


def worker_thread(the_queue):
    """Worker thread, for each instance
    """
    global log
    log.info('Waiting for tasks')
    while True:
        try:
            job = the_queue.get_nowait()
        except queue.Empty:
            log.info('Nothing else to do. Exiting')
            break
        log.info('Got a task %s', job.get_id())

        ret_code = job.run()
        if ret_code != 0:
            log.warning('Unexpected exit code: %d', ret_code)
            stdout = job.get_stdout()
            stderr = job.get_stderr()
            if stdout is not None:
                log.error('STDOUT: %s', stdout)
            if stderr is not None:
                log.error('STDERR: %s', stderr)
            # Notify the queue that this job has ended, but reenqueue it
            the_queue.task_done()
            the_queue.put(job)
            continue

        log.debug('Task execution finished')

        IDS_DONE.append(job.get_id())

        the_queue.task_done()
        log.debug('Task succesfully completed')

    log.info('Thread exiting.')


def exit_with_error(msg, code):
    """Exits this program immediately with a given exit code

    Args:
        msg (str): An optional message to display before exit
        code (int): The exit code
    """
    print(msg)
    exit(code)


def read_csv_into_queue(config, the_queue):
    """Reads a given csv file into a queue of Task objects

    Args:
        csvfile (str): The path to the desired csv file to load
        the_queue (Queue): A queue from the queue module
    """
    log.info('Populating queue')
    task_creator = config.get('coordinator', 'taskcreator')
    csv_file = config.get('coordinator', 'csvfile')
    log.debug('The file will be: {}'.format(csv_file))
    creator_class = getattr(sys.modules[__name__], task_creator)
    creator = creator_class(csv_file, config)
    tasks = creator.create_tasks()
    log.debug('Pushing {} tasks into queue'.format(len(tasks)))
    for jsondesc in tasks:
        t = task.Task(jsondesc)
        the_queue.put(t)


def main():
    """Main function

    Raises:
        FileNotFoundError: In case the config file doesn't exists
    """
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
    read_csv_into_queue(cfg, QUEUE)
    for i in range(int(workers)):
        thread = threading.Thread(target=worker_thread, args=(QUEUE,))
        thread.setName('worker-{}'.format(i))
        thread.start()
        threads.append(thread)

    QUEUE.join()
    log.info('List of ids done: {}'.format(IDS_DONE))
    for t in threads:
        t.join()


if __name__ == '__main__':
    main()
