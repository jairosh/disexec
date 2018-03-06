# -*- coding: utf-8 -*-
# @Author: Jairo Sanchez
# @Date:   2018-03-01 16:06:35
# @Last Modified by:   Jairo Sanchez
# @Last Modified time: 2018-03-05 18:40:00
import json
import os
import tempfile
import subprocess


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
            self._folderpath = self._data['external_data_folder']
        # TODO: Manage the exception if it can't create the directory
        else:
            self._tempfolder = tempfile.TemporaryDirectory()
            self._folderpath = self._tempfolder.name

        external_data = os.path.join(self._folderpath, str(self._data['id']) + '.txt')
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
        subprocess.run(cmd, cwd=os.path.dirname(self._data['command']))
        print('Running:{0} {1}'.format(self._data['command'], self._arguments))
        self.clean()

    def __str__(self):
        rep = 'Data={0}\n'.format(self._data)
        return rep
