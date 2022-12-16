# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
from itemadapter import ItemAdapter
#
#
# class IndomioPipeline:
#     def process_item(self, item, spider):
#         return item

import scrapy
from itemadapter import ItemAdapter
from scrapy.exceptions import DropItem
from scrapy.pipelines.images import ImagesPipeline
from slugify import slugify


class MyImagesPipeline(ImagesPipeline):
    def get_media_requests(self, item, info):
        for index, image_url in enumerate(item['image_urls']):
            yield scrapy.Request(image_url, meta={'image_name': item["Name"],
                                                  'index': index})

    def file_path(self, request, response=None, info=None, *, item=None):
        return '%s.jpg' % slugify(f"{request.meta['image_name']}_{request.meta['index']}")

    def item_completed(self, results, item, info):
        image_paths = [x['path'] for ok, x in results if ok]
        if not image_paths:
            raise DropItem("Item contains no images")
        adapter = ItemAdapter(item)
        adapter['image_paths'] = image_paths
        return item
