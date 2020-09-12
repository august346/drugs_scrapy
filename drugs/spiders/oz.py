import json
from functools import reduce

import scrapy

from drugs.db import models
from drugs.utils import base_transformer, utils

REQUEST_QUERY_FP = 'drugs/src/oz/oz.query'
SRC_URL_MASKED = '68747470733a2f2f7777772e7269676c612e72752f6772617068716c'


class OzTransformer(base_transformer.Transformer):
    def get_transformed_item(self):
        return self._oz_extract_data(
            self._item,
            *map(self._get_param, ('id', 'name', 'sku', 'mnn_ru', 'promo_label')),
            *map(self._get_bool_param, (
                ('is_active', 'active'),
                ('is_receipt', 'rec_need'),
                ('is_delivery', 'delivery'),
                ('is_in_stock', 'is_in_stock'),
                ('is_thermolabile', 'thermolabile'),
            )),
            *map(self._get_deeper_param, (
                ('manufacturer_id', ('manufacturer_id', 'option_id')),
                ('manufacturer', ('manufacturer_id', 'label')),
                ('manufacturer_ru', ('manufacturer_ru', 'label')),
                ('price', ('price', 'regularPrice', 'amount', 'value')),
                ('images', ('media_gallery', 0)),
                ('original', ('orig_preparat', 'label')),
            )),
            ('forms_url', lambda src: src['lekforms_url'], self._oz_extract_url),
            ('categories', lambda src: src['breadcrumbs'], self._oz_edit_categories),
            ('spec_attributes', lambda src: src['specification_set_attributes'], self._oz_extract_attrs),
            ('desc_attributes', lambda src: src['description_set_attributes'], self._oz_extract_attrs),
        )

    @staticmethod
    def _oz_extract_data(src_dict, *update_parts_args):
        return {
            key: (update_func or (lambda x: x))(extract_func(src_dict))
            for key, extract_func, update_func in update_parts_args
        }

    @staticmethod
    def _get_param(key):
        return key, lambda x: x[key], None

    @staticmethod
    def _get_deeper_param(args):
        key, path = args

        def get_deeper(x, x_keys):
            if not x_keys:
                return x
            if x in (None, (), [], {}):
                return None
            return get_deeper(x[x_keys[0]], x_keys[1:])

        return key, lambda x: get_deeper(x, path), None

    @staticmethod
    def _get_bool_param(args):
        res_key, src_key = args

        return res_key, lambda x: x[src_key], lambda x: x == 'true' or bool(int(x))

    @staticmethod
    def _oz_extract_url(extracted):
        if not extracted:
            return extracted

        return extracted.split('||')[1]

    @staticmethod
    def _oz_extract_attrs(extracted):
        def _oz_extract_one(attr_struct):
            attr_label, values = map(attr_struct.get, ('attribute_label', 'values'))
            values = tuple(value.get('value') for value in values)
            return attr_label, values
        return dict(map(_oz_extract_one, extracted))

    @staticmethod
    def _oz_edit_categories(extracted):
        def extract_category(category_struct):
            if (category_id := category_struct.get('id')) in ('2669', '2672', '2671'):
                return None
            return category_id, category_struct.get('name')

        def add_categories(dict_to_upd: dict, categories_struct: dict) -> dict:
            dict_to_upd.update(filter(
                lambda x: x is not None,
                map(extract_category, categories_struct.get('path'))
            ))
            return dict_to_upd

        return reduce(add_categories, json.loads(extracted), dict())


class OzSpider(scrapy.Spider):
    name = 'oz'
    transformer = OzTransformer
    db_model = models.OzDrug

    download_delay = 0.5

    def __init__(self, page_size=20, page_limit=626, *args, **kwargs):
        super(OzSpider, self).__init__(*args, **kwargs)
        self.page_size = int(page_size)
        self.page_limit = int(page_limit)

        self._url = None
        self._query_template = None

    @property
    def url(self):
        if self._url is None:
            self._url = utils.decode(SRC_URL_MASKED, 'hex')
        return self._url

    @property
    def query(self):
        with open(REQUEST_QUERY_FP, 'r', encoding='utf-8') as file:
            self._query_template = file.read()
        return self._query_template

    def start_requests(self):
        for page_num in range(1, self.page_limit):
            yield scrapy.http.JsonRequest(
                url=self.url,
                data=self.request_json(page_num),
                cb_kwargs ={'page': page_num},
                callback=self.parse
            )

    def request_json(self, page_num):
        return {
            'query': self.query,
            'variables': {
                'page': page_num,
                'size': self.page_size
            }
        }

    def parse(self, response, **kwargs):
        rsp_json = response.json()
        batch = rsp_json.get('data', {}).get('productDetail', {})
        batch.update(response.cb_kwargs)
        return batch

    @staticmethod
    def get_item_id(item):
        return item.get('id')

    def save(self, session, batch):
        items = batch['items']
        items_ids = tuple(self.get_item_id(i) for i in items)

        in_table_already_ids = tuple(
            oz_drug.id for oz_drug
            in (
                session.query(self.db_model)
                .filter(self.db_model.id.in_(items_ids))
                .all()
            )
        )
        session.bulk_save_objects(
            models.OzDrug(id=id_, data=item)
            for item in batch['items']
            if (id_ := self.get_item_id(item)) not in in_table_already_ids
        )
        session.commit()

        return 'page: {}/{}\tadded: {}/{}'.format(
            batch['page'],
            self.page_limit - 1,
            self.page_size - len(in_table_already_ids),
            self.page_size
        )
