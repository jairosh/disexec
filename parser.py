#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# @Author: Jairo Sanchez
# @Date:   2018-03-08 14:09:18
# @Last Modified by:   Jairo Sánchez
# @Last Modified time: 2018-07-07 12:44:46
import re
import os


class Parser(object):
    """Abstract class for a generic file parser into a JSON representation
    """

    def __init__(self, filename):
        self._file = filename

    def get_results(self):
        raise NotImplementedError('Invoked the base method. You should ' +
                                  'implement your own method')


class MessageStatsReportParser(Parser):
    """Reads the content of a MessageStatsReport file from ONE and returns it
    in JSON formatted
    """

    def __init__(self, filepath):
        """Constructor

        Args:
            filepath (str): The path to the report file to be parsed
        """
        self._file = filepath
        self._dict = None

    def get_results(self):
        """Parses the content of the report file and generates a dict structure

        Returns:
            dict: The dictionary with all the stats parsed from the file

        Raises:
            FileNotFoundError: In case the provided report path didn't exists
        """
        if self._dict:
            return self._dict
        self._dict = {}
        if os.path.exists(self._file):
            with open(self._file, 'r') as report:
                first_line = True
                for line in report:
                    if first_line:
                        self._dict['id'] = re.compile('.*scenario (.*)')\
                                             .match(line).groups()[0]
                        self._dict['scenario'] = re.compile('.*scenario (.*)')\
                                                   .match(line).groups()[0]
                        first_line = False
                        continue
                    fields = line.strip().split(':')
                    self._dict[fields[0]] = self._parse_value(fields[1])
        else:
            raise FileNotFoundError('The provided path doesn\'t exists:{0}'
                                    .format(self._file))
        return self._dict.update(self._parse_filename())

    def _parse_filename(self):
        fn = os.path.basename(self._file)
        filename_regex = re.compile('(.*)_(.*)Router_(\d+)n_\[(\d+)\]ttl_' +
                                    '\[(\d+)\]s_seed\[(.*)\]_\[(\d+)M\]_' +
                                    '\[(.*)\]_w\[(\d+)\]_MetricsReport.txt')
        print('Matching ' + fn)
        match = re.match(filename_regex, fn)
        print(match)
        if match:
            return {'mobility': match.groups()[0],
                    'router': match.groups()[1],
                    'nodes': match.groups()[2],
                    'ttl': match.groups()[3],
                    'seed': match.groups()[5],
                    'buffer_size': match.groups()[6],
                    'message_interval': match.groups()[7],
                    'exp_weight': match.groups()[8]}

    def _parse_value(self, value_string):
        """Tries to parse :value_string: guessing the data type

        Args:
            value_string (str): The string representation to parse

        Returns:
            object: The value in the corresponding type (str, float, int)
        """
        integer = re.compile('^[+-]?\d+$')
        floatno = re.compile('^[+-]?[\d]+[.][\d]+$')
        value = None
        value_string = value_string.strip()
        if integer.match(value_string):
            value = int(value_string)
        else:
            if floatno.match(value_string):
                value = float(value_string)
            else:
                value = value_string
        return value
