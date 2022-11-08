import configparser
import os
import json
from pathlib import Path
from datetime import date
from collections import Counter

from manatus.source_resource import DPLARecordEncoder

CONFIG_PATH = Path(os.getenv('MANATUS_CONFIG'))
manatus_config = configparser.ConfigParser()
manatus_config.read(os.path.join(CONFIG_PATH, 'manatus.cfg'))

harvest_parser = configparser.ConfigParser()
harvest_parser.read(os.path.join(CONFIG_PATH, 'manatus_harvests.cfg'))

scenario_parser = configparser.ConfigParser()
scenario_parser.read(os.path.join(CONFIG_PATH, 'manatus_scenarios.cfg'))

OAI_PATH = os.path.abspath(manatus_config['ssdn']['InFilePath'])
JSONL_PATH = f"{manatus_config['ssdn']['OutFilePath']}/{manatus_config['ssdn']['OutFilePrefix']}-{date.today()}.jsonl"


def list_config_keys(config_parser):
    return [section for section in config_parser.sections()]


def add_json(source, target):
    with open(target, 'a', encoding='utf-8', newline='\n') as json_out:
        source_file = open(source)
        source_json = json.load(source_file)
        for rec in source_json:
            json_out.write(f"{json.dumps(rec, cls=DPLARecordEncoder)}\n")
        source_file.close()


def count_records(fp):
    data_providers = []

    with open(fp) as f:
        for line in f:
            rec = json.loads(line)
            data_providers.append(rec['dataProvider'])

    counts = Counter(data_providers)

    for item in list(counts):
        print(item, ': ', counts[item])


def _rec_gen(source_file):
    """
    :param source_file: JSON file of SSDN records
    :return: Generator or records in source_file
    """
    with open(source_file + ".bak") as f:
        print(f"Calling: dpla_local_subjects 3")
        for line in f:
            yield json.loads(line)


def _sub_gen(rec):
    """
    :param rec: JSON record
    :return: Generator of subjects in rec
    """
    print(f"Calling: dpla_local_subjects 4")
    try:
        for sub in rec['sourceResource']['subject']:
            yield sub
    except KeyError:
        pass


def dpla_local_subjects(fp):
    import shutil
    from dpla_local_map import dpla_local_map

    shutil.move(fp, fp + ".bak")
    out = open(fp, 'a', encoding='utf8', newline='\n')
    print(f"Calling: dpla_local_subjects 2")
    for rec in _rec_gen(fp):
        for sub in _sub_gen(rec):
            '''
            check existing subjects against mapped terms 
            and make sure supplied subject isn't already
            in record
            '''
            if sub['name'] in dpla_local_map.keys() and dpla_local_map[sub['name']][0][0] not in [term['name'] for term
                                                                                                  in
                                                                                                  rec['sourceResource'][
                                                                                                      'subject']]:
                if len(dpla_local_map[sub['name']]) > 1:
                    for item in dpla_local_map[sub['name']]:
                        rec['sourceResource']['subject'].append({'name': item[0], "@id": item[1]})
                    break
                else:
                    rec['sourceResource']['subject'].append(
                        {'name': dpla_local_map[sub['name']][0][0], "@id": dpla_local_map[sub['name']][0][1]})
                    break
        out.write(json.dumps(rec) + '\n')
