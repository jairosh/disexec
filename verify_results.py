# -*- coding: utf-8 -*-
# @Author: Jairo Sánchez
# @Date:   2018-03-20 13:50:24
# @Last Modified by:   Jairo Sánchez
# @Last Modified time: 2018-04-26 16:07:53
import amqpstorm
import configparser
import argparse
import os
import csv
import re
import json


DEFAULT_CONFIG_FILE = './disexec.config'


def load_results(queue_url, queue_name):
    """Load the JSON objects that represent a result in the provided queue

    Args:
        queue_url (str): The URI for the results queue
        queue_name (str): The name of the queue

    Returns:
        list: Returns a list of dicts, where each one contains the results
    """
    connection = amqpstorm.UriConnection(queue_url)
    channel = connection.channel()
    channel.queue.declare(queue_name, durable=True)
    received_ids = []
    data = []
    while True:
        message = channel.basic.get(queue=queue_name, no_ack=False)
        if message is None:
            break
        result = json.loads(message.body)
        if any(result):
            received_ids.append(message)
            data.append(result)
    for m in received_ids:
        m.nack()
    connection.close()
    return data


def load_experiments(csvfile):
    """Load the experiments specified in the CSV file

    Args:
        csvfile (str): Path to the csv file

    Returns:
        list: List of dicts, each one is a specification of a experiment
    """
    experiments = []
    try:
        with open(csvfile) as exp_csv:
            exp_reader = csv.reader(exp_csv)
            for row in exp_reader:
                experiments.append(row)
    except Exception as e:
        exit_with_error(e)

    return format_as_dict(experiments)


def format_as_dict(experiments):
    """Format the provided list of lists as a dict. The first item is the
    list with each filed name, i.e. the header.

    Args:
        experiments (list): A list containing lists. the first one has the
        field names and the rest are data

    Returns:
        list: A list of dicts, each one includes the name for each field
    """
    header = experiments[:1][0]
    data = []
    for exp in experiments[1:]:
        if len(header) != len(exp):
            exit_with_error('Mismatch on columns number')
        experiment = dict(zip(header, exp))
        data.append(experiment)
    return data


def evaluate_filenames(experiments):
    """Checks for the filename template and path and populates it with the
    available data. The format and template used here is the same as the ONE

    Args:
        experiments (list): List of dicts containing a row of the original CSV
    """
    paths = []
    suffix = '_MessageStatsReport.txt'
    for experiment in experiments:
        if experiment['Scenario.name'] is None:
            exit_with_error('Template error. Scenario.name not in the file', 4)
        scenarios = format_template(experiment['Scenario.name'], experiment)

        if experiment['Report.reportDir'] is None:
            exit_with_error('Report.reportDir is undefined', 7)
        parent_dir = experiment['Report.reportDir']
        for scen in scenarios:
            paths.append(os.path.join(parent_dir, scen + suffix))

    return paths


def format_template(template, values, formatter='%%'):
    """Replaces parameters surrounded by %% with their respective value

    Args:
        template (str): A formatting string
        values (dict): The collection of values to populate in the template

    Returns:
        list: List of formatted strings
    """
    pattern = re.compile('.*{0}(\w+\.\w+){0}.*'.format(formatter))
    formatted = []
    while pattern.match(template):
        match = pattern.match(template)
        name = match.groups()[0]
        value = values[name]
        if value.startswith("[") and value.endswith("]"):
            value = value.replace("[", "").replace("]", "")
            all_values = value.split(";")
            for val in all_values:
                sub_str = template.replace('{0}{1}{0}'.format(formatter, name),
                                           val.strip())
                formatted += format_template(sub_str, values, formatter)
            return formatted
        else:
            template = template.replace('{0}{1}{0}'.format(formatter, name),
                                        values[name])

    formatted.append(template)
    return formatted


def get_scenario_name(experiment, ignorelist):
    """Formats the string with the scenario name for the experiment with the
    parameters that are provided for the experiment.

    Args:
        experiment (dict): The dict with the representation of the experiment.
        Must have the Scenario.name key, and every value that is between %%
        ignorelist (list): For each string that is here, it will be replaced by
        the regex .* to allow repetition e.g. an rng seed

    Returns:
        str: The corresponding scenario name
    """
    if experiment['Scenario.name']:
        name = experiment['Scenario.name']
        # Parameters are presented berween four percentile (%%{param}%%)
        pattern = re.compile('%%(.*?)%%')
        for var in pattern.findall(name):
            if var in ignorelist:
                name = name.replace('%%{}%%'.format(var), '.*')
                continue
            name = name.replace('%%{0}%%'.format(var),
                                experiment[var])

        return name
    else:
        return None


def write_to_file(name, contents):
    """Writes content to a file

    Args:
        name (str): The path to the file
        contents (object): The object to write
    """
    with open(name, 'w') as output:
        output.writelines(contents)


def check_experiment_in_results(exp, results, executions, ignorelist):
    """Validates that the experiment has complete results

    Args:
        exp (dict): The description of the experiment
        results (list): List of dict containing all the results
        executions (int): Expected number of individual results
        ignorelist (list): The list of parameters to ignore in the repetition
        pattern

    Returns:
        bool, float: A booloean flag indicating if the expected results are
        found, the ratio between the results that match the scenario name
        with the expected results
    """
    scenario = get_scenario_name(exp, ignorelist)
    # Escape characters in name to form a valid regex
    pattern = re.compile(scenario.replace('[', '\[')
                                 .replace(']', '\]'))
    finished = []
    for res in results:
        if 'id' not in res:
            continue
        if pattern.match(res['id']):
            finished.append(res)
    complete = len(finished) == executions
    progress = float(len(finished)) / float(executions)
    if not complete:
        if len(finished) == 0:
            print('{} NONEXEC'.format(scenario))
        elif len(finished) < executions:
            print('{0} INCOMPLETE ({1})'.format(scenario, progress))
        elif len(finished) > executions:
            print('{0} REPEATED '.format(scenario))
            write_to_file(scenario, [json.dumps(x) for x in finished])
    return complete, progress


def exit_with_error(msg, code=1):
    print(msg)
    exit(1)


def main():
    desc = 'Verify the results in a queue with the CSV file that was used ' +\
           'to generate the experiments'
    parser = argparse.ArgumentParser(description=desc)
    parser.add_argument('-c', '--config', type=str,
                        help='Use this configuration file')
    parser.add_argument('-r', '--results', type=int, default=1,
                        help='Expected number of results per line in the CSV')
    parser.add_argument('-i', '--ignore', nargs='+',
                        help='If results are >1, this indicates the parameter\
                              string to skip in the scenario name e.g. seed')
    parser.add_argument('-l', '--local', action='store_true', default=False,
                        help='Resolves filenames and checks if exist')

    args = parser.parse_args()
    config_file = DEFAULT_CONFIG_FILE
    if args.config:
        if os.path.isfile(args.config):
            config_file = args.config
    cfg = configparser.RawConfigParser()
    try:
        if not os.path.isfile(config_file):
            raise FileNotFoundError('Unable to locate the config file')
        cfg.read(config_file)
    except Exception as e:
        exit_with_error(e)

    experiments = load_experiments(cfg.get('coordinator', 'csvfile'))

    if args.local:
        paths = evaluate_filenames(experiments)
        for file in paths:
            print('{0}, "{1}"'.format(os.path.exists(file), file))
        exit(0)

    res = load_results(cfg.get('worker', 'queue_url'),
                       cfg.get('general', 'results_queue_name'))

    valid_exp = []
    rerun = []
    for experiment in experiments:
        complete, progress = check_experiment_in_results(experiment, res,
                                                         args.results,
                                                         args.ignore)
        if complete:
            valid_exp.append(experiment)
        else:
            rerun.append(experiment)

    if len(rerun) > 0:
        with open('rerun.csv', 'w') as output:
            keys = rerun[0].keys()
            writer = csv.DictWriter(output, fieldnames=keys)
            writer.writeheader()
            writer.writerows(rerun)


if __name__ == '__main__':
    main()
