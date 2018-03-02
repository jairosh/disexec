# -*- coding: utf-8 -*-
# @Author: Jairo Sanchez
# @Date:   2018-03-01 16:06:35
# @Last Modified by:   Jairo Sanchez
# @Last Modified time: 2018-03-01 17:44:29
import json
import os
import tempfile


class Task(object):
    """Represents a Task to be created by a coordinator, and to be given to
    the workers to be executed"""

    def __init__(self, jsondesc):
        """Creates a Task object from a JSON string (also see from_file)"""
        self._data = json.loads(jsondesc)
        self._folderpath = None
        self._tempfolder = None
        self._arguments = ''
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
                self._folder = self._data['external_data_folder']
        else:
            self._tempfolder = tempfile.TemporaryDirectory()
            self._folder = self._tempfolder.name

        external_data = os.path.join(self._folder, self._data['id'])
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

    def main(self):
        """The main phase of this task, this is where the hevy lifting is done.
        """
        self.prepare()
        print('Running:{0} {1}'.format(self._data['command'], self._arguments))
        self.clean()
