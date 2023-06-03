# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
from itemadapter import ItemAdapter
import pymongo
from urllib.parse import quote_plus
import os
from dotenv import load_dotenv
import logging

load_dotenv()


class ItemCheckPipeline:
    def process_item(self, item, spider):
        item = ItemAdapter(item)
        length = [
            len(item.get("datetimes")),
            len(item.get("usernames")),
            len(item.get("user_post_count")),
            len(item.get("bodys")),
            len(item.get("replies")),
        ]
        if length.count(length[0]) != len(length):
            spider.log(f"{item.get('url')} - {length}", logging.ERROR)


class MongoPipeline:
    def __init__(self, mongo_host: str, mongo_db: str, collection_name: str):
        self.mongo_host = mongo_host
        self.mongo_db = mongo_db
        self.collection_name = collection_name

    @classmethod
    def from_crawler(cls, crawler):
        return cls(
            mongo_host=os.environ.get("MONGO_HOST"),
            mongo_db=os.environ.get("MONGO_DATABASE"),
            collection_name=crawler.spider.collection_name,
        )

    def open_spider(self, spider):
        try:
            self.client = pymongo.MongoClient(
                self.mongo_host,
                username=quote_plus(os.environ.get("MONGO_USERNAME")),
                password=quote_plus(os.environ.get("MONGO_PASSWORD")),
                retryWrites=True,
            )
            self.collection = self.client[self.mongo_db][self.collection_name]
            self.client.admin.command("ping")
        except Exception as e:
            spider.log(e, logging.ERROR)

    def close_spider(self, spider):
        self.client.close()

    def process_item(self, item, spider):
        discussion = ItemAdapter(item)
        for i in range(0, len(discussion["replies"])):
            self.collection.insert_one(
                {
                    "title": discussion.get("title"),
                    "post_id": discussion.get("post_id"),
                    "url": discussion.get("url"),
                    "datetime": discussion.get("datetimes")[i],
                    "username": discussion.get("usernames")[i],
                    "user_post_count": discussion.get("user_post_count")[i],
                    "body": discussion["bodys"][i],
                    "reply": discussion.get("replies")[i],
                }
            )
        return item
