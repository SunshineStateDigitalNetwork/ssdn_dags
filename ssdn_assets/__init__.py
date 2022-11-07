import configparser
import os
import json
from pathlib import Path
from datetime import date

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
            json_out.write(json.dumps(rec), + '\n')
        source_file.close()
