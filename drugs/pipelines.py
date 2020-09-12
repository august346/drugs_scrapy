# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html

from drugs.db import db
from drugs.utils import utils


class DrugsPipeline:
    storage_dir = 'drugs/src/oz/results'

    def __init__(self, pg_url):
        self.pg_url = pg_url

        self.sqlalchemy = None

    @classmethod
    def from_crawler(cls, crawler):
        pg_url = '{}://{}:{}@{}:{}/{}'.format(
            *(utils.get_config_var(crawler, var_name) for var_name in (
                'PG_DRIVER',
                'PG_USERNAME',
                'PG_PASSWORD',
                'PG_HOST',
                'PG_PORT',
                'PG_DB_NAME'
            ))
        )
        return cls(pg_url=pg_url)

    def open_spider(self, spider):
        self.sqlalchemy = db.SQLAlchemy(self.pg_url)

    def close_spider(self, spider):
        self.sqlalchemy.close()

    def process_item(self, item, spider):
        to_log = spider.save(self.sqlalchemy.session, item)
        return to_log
