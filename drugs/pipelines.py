# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
import os

from itemadapter import ItemAdapter

from drugs import utils


class DrugsPipeline:
    storage_dir = 'drugs/src/oz/results'

    def process_item(self, item, spider):
        name = '{}_{}'.format(spider.name, spider.get_item_id(item))
        filepath = os.path.join(self.storage_dir, '{}.json'.format(name))

        transformed_item = spider.transformer(item).get_transformed_item()

        utils.save_json(filepath, transformed_item)
        return name
