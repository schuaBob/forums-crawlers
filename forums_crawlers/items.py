# Define here the models foField()r your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

from scrapy import Field, Item
from itemloaders.processors import TakeFirst, MapCompose, Compose
from w3lib.html import remove_tags, replace_entities, replace_escape_chars, strip_html5_whitespace
from forums_crawlers.processors import to_int, is_reply

class Discussion(Item):
    title = Field(input_processor=TakeFirst())
    post_id = Field(input_processor=TakeFirst())
    url = Field(input_processor=TakeFirst())
    datetimes = Field()
    usernames = Field()
    user_post_count = Field(input_processor=MapCompose(to_int))
    bodys = Field(input_processor=MapCompose(remove_tags, replace_entities, replace_escape_chars, strip_html5_whitespace))
    replies = Field(input_processor=Compose(is_reply))
