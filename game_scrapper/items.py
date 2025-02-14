# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy


class IgdbItem(scrapy.Item):
    name = scrapy.Field()
    cover = scrapy.Field()
    consoles = scrapy.Field()
    first_release_date = scrapy.Field()
    categories = scrapy.Field()
    screenshots = scrapy.Field()
    storyline = scrapy.Field()
    summary = scrapy.Field()

class ScrappedItem(scrapy.Item):
    price = scrapy.Field()
    game_console_id = scrapy.Field()
    url = scrapy.Field()
    product_id = scrapy.Field()