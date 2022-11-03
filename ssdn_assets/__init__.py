import configparser
import os
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
