import configparser
import os
import json
import logging
from pathlib import Path
from datetime import date
from collections import Counter

from manatus.source_resource import DPLARecordEncoder

logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())

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
    """

    :param config_parser:
    :return:
    """
    return [section for section in config_parser.sections()]


def add_json(source, target):
    """
    Add JSON from source file to target file as JSONL
    :param source: Path to JSON document
    :param target: JSONL output document
    :return:
    """
    logger.info(f"Adding {source} JSON to {target}")
    with open(target, 'a', encoding='utf-8', newline='\n') as json_out:
        source_file = open(source)
        source_json = json.load(source_file)
        logger.debug(f"Loaded {source} as JSON to add to {target}")
        for rec in source_json:
            json_out.write(f"{json.dumps(rec, cls=DPLARecordEncoder)}\n")
        source_file.close()
        logger.debug(f"Added {source} to {target}")


def count_records(fp):
    """
    Count record contributions by provider
    :param fp: Path to JSONL document
    :return:
    """
    data_providers = []

    with open(fp) as f:
        logger.info(f"Reading {fp} to count records")
        for line in f:
            rec = json.loads(line)
            data_providers.append(rec['dataProvider'])

    logger.debug(f"Counting records - {fp}")
    counts = Counter(data_providers)

    logger.debug(f"Printing record count - {fp}")
    for item in list(counts):
        print(item, ': ', counts[item])


def _rec_gen(fp):
    """
    :param fp: JSON file of SSDN records
    :return: Generator or records in source_file
    """
    with open(fp) as f:
        logger.debug(f"Record iterator for {fp}")
        for line in f:
            yield json.loads(line)


def _sub_gen(rec):
    """
    :param rec: JSON record
    :return: Generator of subjects in rec
    """
    logger.debug(f"Subject iterator for - {rec['isShownAt']}")
    try:
        for sub in rec['sourceResource']['subject']:
            yield sub
    except KeyError:
        pass


def dedupe_records(fp):
    """

    :param fp: Path to JSONL document
    :return:
    """
    import shutil

    logger.info(f"Deduping records = {fp}")
    logger.info(f"Backing up {fp} to {fp}.bak")
    shutil.move(fp, fp + ".bak")
    seen = []
    out = []
    for rec in _rec_gen(fp):
        if rec['isShownAt'] not in seen:
            seen.append(rec['isShownAt'])
            out.append(rec)
        else:
            logger.__setattr__("provider", str(rec['dataProvider']))
            logger.error('Duplicate record - {}'.format(rec['isShownAt']))

    with open(fp, 'a', encoding='utf8', newline='\n') as out_fp:
        for rec in out:
            out_fp.write(json.dumps(rec) + '\n')


def dpla_local_subjects(fp):
    import shutil
    from .dpla_local_map import dpla_local_map

    logger.info(f"Backing up {fp} to {fp}.bak")
    shutil.move(fp, fp + ".bak")
    out_fp = open(fp, 'a', encoding='utf8', newline='\n')
    for rec in _rec_gen(fp + '.bak'):
        logger.debug(f"Checking {rec['isShownAt']}")
        for sub in _sub_gen(rec):
            logger.debug(f"Checking {sub}")
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
                        logger.info(f"Adding {item[0]} & {item[0]} to {rec['isShownAt']}")
                        rec['sourceResource']['subject'].append({'name': item[0], "@id": item[1]})
                    break
                else:
                    logger.info(f"Adding {dpla_local_map[sub['name']][0][0]} & {dpla_local_map[sub['name']][0][1]} to {rec['isShownAt']}")
                    rec['sourceResource']['subject'].append(
                        {'name': dpla_local_map[sub['name']][0][0], "@id": dpla_local_map[sub['name']][0][1]})
                    break
        out_fp.write(json.dumps(rec) + '\n')
