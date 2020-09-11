import scrapy

from drugs import utils

REQUEST_QUERY_FP = 'drugs/src/oz/oz.query'
SRC_URL_MASKED = '68747470733a2f2f7777772e7269676c612e72752f6772617068716c'


class OzSpider(scrapy.Spider):
    name = 'oz'

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
                callback=self.parse
            )

    def request_json(self, page_num):
        return {
            'query': self.query,
            'variables': {
                'page': page_num,
                'page_size': self.page_size
            }
        }

    def parse(self, response, **kwargs):
        rsp_json = response.json()
        rs = rsp_json.get('data', {}).get('productDetail', {}).get('items', [])
        return rs
