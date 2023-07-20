# Define here the models foField()r your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

from scrapy import Field, Item
from itemloaders.processors import TakeFirst, MapCompose
from w3lib.html import (
    remove_tags,
    replace_entities,
    replace_escape_chars,
    strip_html5_whitespace,
)
from forums_crawlers.processors import to_int, is_reply, IDtoPDFUrl
from dotenv import load_dotenv
import dateparser

load_dotenv()


class Discussion(Item):
    title = Field(output_processor=TakeFirst())
    post_id = Field(output_processor=TakeFirst())
    url = Field(output_processor=TakeFirst())
    datetimes = Field(input_processor=MapCompose(dateparser.parse))
    user_ids = Field()
    user_post_count = Field(input_processor=MapCompose(to_int))
    bodys = Field(
        input_processor=MapCompose(
            remove_tags, replace_entities, replace_escape_chars, strip_html5_whitespace
        )
    )
    replies = Field(input_processor=is_reply)


class Information(Item):
    title = Field(output_processor=TakeFirst())
    authors = Field(output_processor=TakeFirst())
    year = Field(input_processor=MapCompose(int), output_processor=TakeFirst())
    id = Field(output_processor=TakeFirst())
    file_urls = Field(input_processor=MapCompose(IDtoPDFUrl()))
    files = Field()
    paragraphs = Field()
    embeddings = Field()
    keywords = Field(output_processor=TakeFirst())
