import scrapy

from drugs.db import models
from drugs.utils import utils
from drugs.utils.base_transformer import Transformer

SRC_URL_MASKED = '68747470733a2f2f7777772e61736e612e72752f'


class AsnaTransformer(Transformer):

    def get_transformed_item(self):
        return {
            **self.title(),
            **self.is_receipt(),
            **self.price(),
            **self.images(),
            **self.info(),
            **self.instructions()
        }

    def title(self):
        return {'title': self.item.css('.product-title h1::text').get().strip()}

    def info(self):
        def param_text(li):
            result = li.css('.param-text::text').get().strip()
            if not result:
                result = li.css('.param-text a::text').get().strip()
            return result

        if not (info_list := self.item.css('.infos')):
            return {'info': None}

        return {
            'info': [
                {
                    'label': li.css('.param::text').get().strip(),
                    'value': param_text(li)
                } for li in info_list[0].css('li')
            ]
        }

    def instructions(self):
        def extract_tag_value(tag):
            if tag.root.tag == 'p':
                return tag.css('::text').get(default='').strip()

            return '\t{}'.format('\n\t'.join(
                li.css('::text').get().strip().replace('\n', ' ')
                for li in tag.css('li')
            ))

        return {
            'instructions': [
                {
                    'label': title.strip(),
                    'value': '\n'.join(filter(
                        lambda x: x,
                        map(extract_tag_value, div.css('p, ul'))
                    ))
                } for div in self.item.css('div.product-information__info__content__block')
                if (title := div.css('h3::text').get()) is not None
            ]
        }

    def price(self):
        price = self.item.css('link[itemprop=price]::attr(content)').get()
        return {'price': float(price.strip()) if price else None}

    def images(self):
        def extract_images(img):
            return {
                'size_{}'.format(index): img_url
                for index, attr_name
                in enumerate(('src', 'data-main1', 'data-main2'), start=1)
                if (img_url := img.attrib.get(attr_name)) is not None
            }

        images = list(map(
            extract_images,
            self.item.css('.js-photos-item-zoom')
        ))

        if not images:
            images = [extract_images(self.item.css('.js-main-item-photo'))]

        return {'images': images}

    def is_receipt(self):
        return {'is_receipt': bool(self.item.css('.item-recipe-line'))}


class AsnaSpider(scrapy.Spider):
    name = "asna"
    transformer = AsnaTransformer

    start_urls = [utils.decode(SRC_URL_MASKED, 'hex')]

    download_delay = 2

    def __init__(self, *args, **kwargs):
        super(AsnaSpider, self).__init__(*args, **kwargs)

        self.db_session = None

    def parse(self, response, **kwargs):
        pharma_group_links = response.xpath("//a[text() = 'Аллергия']/../../..//a")
        yield from response.follow_all(pharma_group_links, self.parse_group)

    def parse_group(self, response):
        group_page_links = response.css('ul.pagination__pages a')
        yield from self.parse_page(response)
        yield from response.follow_all(group_page_links, self.parse_page)

    def parse_page(self, response):
        drug_links = response.css('.product__information meta::attr(content)').getall()
        yield from response.follow_all(drug_links, self.parse_drug)

    def parse_drug(self, response):
        item = self.transformer(response).get_transformed_item()
        return item

    def save(self, item):
        drug = models.AsnaDrug(**item)
        self.db_session.add(drug)
        self.db_session.commit()

        return drug.title
