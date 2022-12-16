import datetime
import re

import scrapy
from scrapy import Request


class IndomioSpider(scrapy.Spider):
    name = 'indomio'
    start_urls = ['https://www.indomio.es/']
    custom_settings = {
        'FEED_URI': r'indomio.csv',
        'FEED_FORMAT': 'csv',
    }

    def parse(self, response, **kwargs):
        first_level_cat = response.css('.nd-grid .nd-tabBar:nth-child(1) a::attr(href)').getall()[:-1]
        for level in first_level_cat:
            yield Request(
                url=response.urljoin(level),
                callback=self.parse_second_level
            )

    def parse_second_level(self, response):
        second_level_cat = response.css('.nd-grid .nd-tabBar:nth-child(2) a::attr(href)').getall()
        second_level_cat.append('https://www.indomio.es/alquiler-casas/#map-list')
        for level in second_level_cat:
            yield Request(
                url=response.urljoin(level),
                callback=self.parse_third_level,
                dont_filter=True
            )

    def parse_third_level(self, response):
        third_level_cat = response.css('.nd-grid .nd-tabBar:nth-child(3) a::attr(href)').getall()
        third_level_cat.append(response.url)
        for level in third_level_cat:
            yield Request(
                url=response.urljoin(level),
                callback=self.parse_fourth_level,
                dont_filter=True
            )
        if len(third_level_cat) == 1:
            yield from self.parse_fourth_level(response)

    def parse_fourth_level(self, response):
        fourth_level_cat = response.css('.nd-grid .nd-tabBar:nth-child(4) a::attr(href)').getall()
        fourth_level_cat.append(response.url)
        for level in fourth_level_cat:
            yield Request(
                url=response.urljoin(level),
                callback=self.parse_listing,
                dont_filter=True
            )
        if len(fourth_level_cat) == 1:
            yield from self.parse_listing(response)

    def parse_listing(self, response):
        all_category = response.css('a.nd-listMeta__link::attr(href)').getall()
        for category in all_category:
            if '/municipios' in category:
                yield Request(url=category,
                              callback=self.parse_municipios)
            else:
                yield Request(url=category,
                              callback=self.parse_house_listing)

    def parse_municipios(self, response):
        province_listing = response.css('.nd-listMeta__item a::attr(href)').getall()
        for listing in province_listing:
            yield Request(response.urljoin(listing),
                          callback=self.parse_house_listing)

    def parse_house_listing(self, response):
        listings = response.css('a.in-card__title::attr(href)').getall()
        for listing in listings:
            yield Request(response.urljoin(listing),
                          callback=self.detail_page)

    def pagination(self, response):
        next_page = response.xpath('//*[contains(@class,"in-pagination__item--current")]/following::a[1]/@href').get()
        if next_page:
            yield Request(
                url=next_page,
                callback=self.parse_house_listing
            )

    def detail_page(self, response):
        title = response.css('.im-titleBlock__title::text').get('')
        price = response.css('.im-mainFeatures__title::text').get('')
        old_price = response.css('.im-mainFeatures__title .im-loweredPrice__price::text').get('')
        features = response.css('.im-mainFeatures .nd-list__item')
        address = ' '.join(response.css('.im-titleBlock__content .im-location *::text').getall())
        description = response.css('.im-description__text::text').get('')
        phone = response.css('[type="tel1"] .im-lead__phone--hidden::attr(href)').get('')
        advertiser_name = response.css('.im-lead__reference p::text').get('')
        latitude = re.findall('"latitude":(.+?),"', response.text)
        longitude = re.findall('"longitude":(.+?),"', response.text)
        room, surface, bath, plant = '', '', '', ''
        for feature in features:
            key = feature.css('.im-mainFeatures__label::text').get('')
            value = ' '.join(''.join(feature.css('.im-mainFeatures__value *::text').getall()).split())
            if 'habitación' in key or 'habitaciones' in key:
                room = value
            if 'superficie' in key:
                surface = value
            if 'baño' in key:
                bath = value
            if 'planta' in key:
                plant = value
        characteristics = response.xpath('//*[contains(@id,"características")]/ancestor::div[1]/following::dl[1]//dt')
        characteristics_list, energy_list = list(), list()
        for characteristic in characteristics:
            key = characteristic.css('::text').get()
            value = characteristic.xpath('.//following::dd[1]//text()').get('').strip()
            characteristics_list.append(f"{key}:{value}")

        energy_efficient = response.xpath('//*[contains(text(),"Eficiencia energética")]/following::dl[1]//dt')
        for energy in energy_efficient:
            key = energy.css('::text').get()
            value = energy.xpath('.//following::dd[1]//text()').get('').strip()
            energy_list.append(f"{key}:{value}")
        images = re.findall(',"large":"(.+?)"', response.text)
        refine_images = [image.replace('\\', '') for image in images if 'xxl' in image]

        yield {
            'Name': title,
            'Address': self.clean(address),
            'Surface': surface,
            'Bedrooms': room,
            'Bath': bath,
            'Plant': plant,
            'Price': self.clean(price),
            'Old Price': self.clean(old_price),
            'Latitude': ''.join(latitude),
            'Longitude': ''.join(longitude),
            'Advertiser Name': advertiser_name,
            'Advertiser Phone': phone.replace('tel:', ''),
            'Description': self.clean(description),
            'Characteristics': ', '.join(characteristics_list),
            'Energy Efficient': ', '.join(energy_list),
            'image_urls': refine_images,
            'Data Source': 'indomio',
            'Date': datetime.datetime.now(),
            'Url': response.url,

        }

    def clean(self, value):
        return ' '.join(value.split()) if value else ''
