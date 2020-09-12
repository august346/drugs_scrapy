import codecs
import json


def decode(encoded, _type):
    return codecs.decode(encoded, 'hex').decode()


def save_json(fp, data):
    with open(fp, 'w', encoding='utf-8') as file:
        json.dump(data, file, indent=2, ensure_ascii=False)


def get_config_var(crawler, var_name):
    return crawler.settings.get(var_name)
