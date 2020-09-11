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
        filename = '{}.json'.format(item.get('id'))
        filepath = os.path.join(self.storage_dir, filename)
        utils.save_json(filepath, item)
        return item
