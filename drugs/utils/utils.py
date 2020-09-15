import codecs


def decode(encoded, _type):
    return codecs.decode(encoded, 'hex').decode()


def get_config_var(crawler, var_name):
    return crawler.settings.get(var_name)
