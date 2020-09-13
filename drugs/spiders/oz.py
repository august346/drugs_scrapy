import json
from functools import reduce

import scrapy
from scrapy.exceptions import CloseSpider

from drugs.db import models
from drugs.utils import base_transformer, utils

REQUEST_QUERY_FP = 'drugs/src/oz/oz.query'
SRC_URL_MASKED = '68747470733a2f2f7777772e7269676c612e72752f6772617068716c'
SRC_APPROXIMATE_PAGE_LIMIT = 700


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

    def __init__(self, page_size=20, *args, **kwargs):
        super(OzSpider, self).__init__(*args, **kwargs)
        self.page_size = int(page_size)

        self._url = None
        self._query_template = None
        self.db_session = None

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
        yield self._get_request(1)

    def parse(self, response, **kwargs):
        rsp_json = response.json()
        batch = rsp_json['data']['productDetail']
        self._close_spider_if_empty(batch['items'])
        batch.update(response.cb_kwargs)
        yield batch
        yield self._get_request(response.cb_kwargs['page'] + 1)

    def save(self, batch):
        items = batch['items']
        ignore_ids = self._get_ignore_ids(items)
        self._add_items(items, ignore_ids)

        return self._get_save_result(batch['page'], ignore_ids)

    def _get_request(self, page_num):
        return scrapy.http.JsonRequest(
            url=self.url,
            data={
                'query': self.query,
                'variables': {
                    'page': page_num,
                    'size': self.page_size
                }
            },
            cb_kwargs={'page': page_num},
            callback=self.parse
        )

    @staticmethod
    def _close_spider_if_empty(items):
        if not items:
            raise CloseSpider('Empty items')

    def _get_ignore_ids(self, items):
        items_ids = tuple(self._get_item_id(i) for i in items)
        already_in_filter = self.db_model.id.in_(items_ids)
        already_in_drugs = self.db_session.query(self.db_model).filter(already_in_filter).all()
        return tuple(oz_drug.id for oz_drug in already_in_drugs)

    def _add_items(self, items, ignore_ids):
        self.db_session.add_all(
            self.db_model(id=id_, data=item)
            for item in items
            if (id_ := self._get_item_id(item)) not in ignore_ids
        )
        self.db_session.commit()

    def _get_save_result(self, page_num, ignore_ids):
        return 'page: {}/~{}\tadded: {}/{}'.format(
            page_num,
            SRC_APPROXIMATE_PAGE_LIMIT,
            self.page_size - len(ignore_ids),
            self.page_size
        )

    @staticmethod
    def _get_item_id(item):
        return item.get('id')
