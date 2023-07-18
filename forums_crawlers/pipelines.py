# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
from itemadapter import ItemAdapter
from urllib.parse import quote_plus
from dotenv import load_dotenv
from scrapy.exceptions import DropItem
from pypdf import PdfReader
import pymongo
import os
from pymilvus import connections, Collection, utility
from scrapy.pipelines.files import FilesPipeline
import scrapy
from sentence_transformers import SentenceTransformer
from tqdm import tqdm
from time import sleep

load_dotenv()


class DownloadPubMedPDF(FilesPipeline):
    def file_path(self, request, response=None, info=None, *, item=None):
        path = os.path.join("pdf", "pubmed", f"{item.get('pmid')}.pdf")
        return path

    def get_media_requests(self, item, info):
        adapter = ItemAdapter(item)
        for file_url in adapter["file_urls"]:
            if file_url:
                yield scrapy.Request(file_url)


class ParsePDFFromFiles:
    def __init__(self, filedir):
        self._filedir = filedir

    @classmethod
    def from_crawler(cls, crawler):
        return cls(crawler.settings.get("FILES_STORE"))

    def process_item(self, item, spider):
        information = ItemAdapter(item)
        if not information["files"]:
            raise DropItem(f"No pmc pdf for pmid:{information.get('pmid')}")

        fullfilename = os.path.join(
            os.path.dirname(os.path.dirname(__file__)),
            self._filedir,
            information["files"][0]["path"],
        )
        reader = PdfReader(fullfilename)
        paragraphs, current_paragraph = list(), list()
        for page in reader.pages:
            text = page.extract_text()
            for line in text.splitlines():
                l = line.strip()
                if not l and current_paragraph:
                    paragraphs.append(" ".join(current_paragraph))
                    current_paragraph.clear()
                elif l.endswith((".", "?", "!")):
                    current_paragraph.append(l)
                    paragraphs.append(" ".join(current_paragraph))
                    current_paragraph.clear()
                else:
                    current_paragraph.append(l)
        if current_paragraph:
            paragraphs.append(" ".join(current_paragraph))
        information["paragraphs"] = paragraphs
        return item


class EncodeTexts:
    def open_spider(self, spider):
        self.model = SentenceTransformer(spider.transformer_name)

    def process_item(self, item, spider):
        information = ItemAdapter(item)
        information["embeddings"] = self.model.encode(
            information["paragraphs"], normalize_embeddings=True
        )
        return item


class MilvusStore:
    def open_spider(self, spider):
        connections.connect(
            alias=os.environ.get("MILVUS_ALIAS"),
            host=os.environ.get("MILVUS_HOST"),
            port=os.environ.get("MILVUS_PORT"),
        )
        if utility.has_collection(spider.collection_name):
            Collection(spider.collection_name).drop()
        self.collection = Collection(spider.collection_name, spider.schema())

    def close_spider(self, spider):
        print(f"Total entities: {self.collection.num_entities}")
        if self.collection.num_entities > 1024:
            self.collection.create_index(
                spider.schema.index_field, spider.schema.index_params
            )
            total = utility.index_building_progress(spider.collection_name)[
                "total_rows"
            ]
            indexed = 0
            with tqdm(total=total, desc="Building Index") as pbar:
                while indexed < total:
                    sleep(0.25)
                    temp = utility.index_building_progress(spider.collection_name)[
                        "indexed_rows"
                    ]
                    n = temp - indexed
                    indexed = temp
                    pbar.update(n)
        utility.wait_for_index_building_complete(spider.collection_name)
        connections.disconnect(self._alias)

    def process_item(self, item, spider):
        info = ItemAdapter(item)
        self.collection.insert(
            [
                [
                    {
                        "url": info["file_urls"][0],
                        "title": info["title"],
                        "authors": info["authors"],
                        "year": info["year"],
                        "paragraph": i + 1,
                    }
                    for i in range(len(info["paragraphs"]))
                ],
                [paragraph for paragraph in info["paragraphs"]],
                [embedding for embedding in info["embeddings"]],
            ]
        )
        self.collection.flush()
        return item


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
            spider.logger.error(e)

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
                    "user_id": discussion.get("user_ids")[i],
                    "user_post_count": discussion.get("user_post_count")[i],
                    "body": discussion["bodys"][i],
                    "reply": discussion.get("replies")[i],
                }
            )
        return item
