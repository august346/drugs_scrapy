import codecs
import json


def decode(encoded, _type):
    return codecs.decode(encoded, 'hex').decode()


def save_json(fp, data):
    with open(fp, 'w', encoding='utf-8') as file:
        json.dump(data, file, indent=2, ensure_ascii=False)
