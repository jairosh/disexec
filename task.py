# -*- coding: utf-8 -*-
# @Author: Jairo Sanchez
# @Date:   2018-03-01 16:06:35
# @Last Modified by:   Jairo Sánchez
# @Last Modified time: 2018-03-26 09:00:21
import json
import os
import tempfile
import subprocess
import parser
import re


class Task(object):
    """Represents a Task to be created by a coordinator, and to be given to
    the workers to be executed"""

    def __init__(self, jsondesc):
        """Creates a Task object from a JSON string (also see from_file)"""
        self._data = json.loads(jsondesc)
        self._folderpath = None
        self._tempfolder = None
        self._arguments = ''
        self._stdout = None
        pass

    @staticmethod
    def from_file(filename):
        """Create a Task object from a JSON file"""
        content = None
        with open(filename, 'r') as jsonFile:
            content = json.load(jsonFile)
        return Task(json.dumps(content))

    def prepare(self):
        """The first phase of the lifecycle. This is executed previous to the
        main phase. It's used for the execution of relevant subtasks e.g.:
        creation of config files, download of dataset, version checks etc."""
        if self._data['external_data_folder']:
            if not os.path.exists(self._data['external_data_folder']):
                os.makedirs(self._data['external_data_folder'])
            self._folderpath = self._data['external_data_folder']
        # TODO: Manage the exception if it can't create the directory
        else:
            self._tempfolder = tempfile.TemporaryDirectory()
            self._folderpath = self._tempfolder.name

        external_data = os.path.join(self._folderpath,
                                     str(self._data['id']) + '.txt')
        with open(external_data, 'w') as fp:
            fp.writelines(self._data['external_data'])

        self._arguments = self._data['arguments'].format(edf=external_data)
        pass

    def clean(self):
        """Last phase of the lifecycle. Called after the completion of the
        main task"""
        if self._tempfolder:
            self._tempfolder.cleanup()
        pass

    def run(self):
        """The main phase of this task, this is where the hevy lifting is done.
        """
        self.prepare()
        cmd = [self._data['command'], ] + self._arguments.split(sep=' ')
        process = subprocess.Popen(cmd,
                                   cwd=os.path.dirname(self._data['command']),
                                   stdout=subprocess.PIPE)
        self._stdout, err = process.communicate()
        result = process.returncode
        self.clean()
        return result

    def result(self):
        """This function opens the result file and reads its contents formatted
        as JSON. Modify according to your needs"""
        output = self._stdout.decode('utf-8')
        pattern = re.compile('^Running simulation \'(.*)\'$')
        files = []
        results = []
        for line in output.split('\n'):
            if pattern.match(line):
                filename = pattern.match(line).groups()[0]
                filename += '_MessageStatsReport.txt'
                files.append(filename)
        pattern = re.compile('^[ ]*Report.reportDir[ ]*=[ ]*(.*)$')
        dirname = ''
        for config in self._data['external_data'].split('\n'):
            if pattern.match(config):
                dirname = pattern.match(config).groups()[0]
                break
        for file in files:
            path = os.path.join(dirname, file)
            json = parser.MessageStatsReportParser(path,
                                                   self._data['id']).get_json()
            results.append(json)
        return results

    def get_id(self):
        return self._data['id']

    def __str__(self):
        return 'Data={0}\n'.format(self._data)
