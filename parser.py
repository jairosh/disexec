#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# @Author: Jairo Sanchez
# @Date:   2018-03-08 14:09:18
# @Last Modified by:   Jairo Sanchez
# @Last Modified time: 2018-03-08 19:09:57
import re
import os
import json


class Parser(object):
    """Abstract class for a generic file parser into a JSON representation"""

    def __init__(self, filename):
        self._file = filename

    def get_json(self):
        raise NotImplementedError('Invoked the base method. You should ' +
                                  'implement your own method')


class MessageStatsReportParser(Parser):
    """Reads the content of a MessageStatsReport file from ONE and returns it
    in JSON formatted """

    def __init__(self, filepath):
        self._file = filepath
        self._json = None

    def get_json(self):
        if self._json:
            return json.dumps(self._json)
        self._json = {}
        if os.path.exists(self._file):
            with open(self._file, 'r') as report:
                first_line = True
                for line in report:
                    if first_line:
                        self._json['id'] = re.compile('.*scenario (.*)')\
                                             .match(line).groups()[0]
                        first_line = False
                        continue
                    fields = line.strip().split(':')
                    self._json[fields[0]] = self._parse_value(fields[1])
        else:
            raise FileNotFoundError('The provided path doesn\'t exists:{0}'
                                    .format(self._file))
        return json.dumps(self._json)

    def _parse_value(self, value_string):
        """Tries to parse the value represented in value_string"""
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
