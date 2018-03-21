# -*- coding: utf-8 -*-
# @Author: Jairo Sánchez
# @Date:   2018-03-20 13:50:24
# @Last Modified by:   Jairo Sánchez
# @Last Modified time: 2018-03-21 00:56:15
import amqpstorm
import configparser
import argparse
import os
import csv
import re
import json


DEFAULT_CONFIG_FILE = './disexec.config'


def load_results(queue_url, queue_name):
    connection = amqpstorm.UriConnection(queue_url)
    channel = connection.channel()
    channel.queue.declare(queue_name, durable=True)
    received_ids = []
    data = []
    while True:
        message = channel.basic.get(queue=queue_name, no_ack=False)
        if message is None:
            break
        received_ids.append(message)
        data.append(message.body)
    for m in received_ids:
        m.nack()
    connection.close()
    return data


def load_experiments(csvfile):
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
    header = experiments[:1][0]
    data = []
    for exp in experiments[1:]:
        if len(header) != len(exp):
            exit_with_error('Mismatch on columns number')
        experiment = dict(zip(header, exp))
        data.append(experiment)
    return data


def get_scenario_name(experiment, blacklist):
    if experiment['Scenario.name']:
        name = experiment['Scenario.name']
        # Parameters are presented berween four percentile (%%{param}%%)
        pattern = re.compile('%%(.*?)%%')
        for var in pattern.findall(name):
            if var in blacklist:
                name = name.replace('%%{}%%'.format(var), '.*')
                continue
            name = name.replace('%%{0}%%'.format(var),
                                experiment[var])

        return name
    else:
        return None


def write_to_file(name, contents):
    with open(name, 'w') as output:
        output.writelines(contents)


def check_experiment_in_results(exp, results, executions, blacklist):
    scenario = get_scenario_name(exp, blacklist)
    # Escape characters in name to form a valid regex
    pattern = re.compile(scenario.replace('[', '\[')
                                 .replace(']', '\]'))
    finished = []
    for res in results:
        result_dic = json.loads(res)
        if 'id' not in result_dic:
            continue
        if pattern.match(result_dic['id']):
            finished.append(result_dic)
    complete = len(finished) == executions
    progress = float(len(finished)) / float(executions)
    if not complete:
        if len(finished) == 0:
            print('{} NONEXEC'.format(scenario))
        if len(finished) < executions:
            print('{0} INCOMPLETE ({1})'.format(scenario, progress))
        if len(finished) > executions:
            print('{0} REPEATED '.format(scenario))
            write_to_file(scenario, finished)
    return complete, progress


def exit_with_error(msg, code=1):
    print(msg)
    exit(1)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('-c', '--config', type=str)
    parser.add_argument('-x', '--executions', type=int, required=True)
    parser.add_argument('-b', '--blacklist', nargs='+')
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
    res = load_results(cfg.get('worker', 'queue_url'),
                       cfg.get('general', 'results_queue_name'))

    valid_exp = []
    rerun = []
    for experiment in experiments:
        complete, progress = check_experiment_in_results(experiment, res,
                                                         args.executions,
                                                         args.blacklist)
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
