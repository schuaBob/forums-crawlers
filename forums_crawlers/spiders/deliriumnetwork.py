import scrapy

from scrapy.linkextractors import LinkExtractor
from scrapy.spiders import CrawlSpider
from urllib.parse import urlencode
from scrapy.loader import ItemLoader
from forums_crawlers.items import Information
from forums_crawlers.pipelines import (
    ParsePDFFromFiles,
    MilvusStore,
    DownloadPubMedPDF,
    EncodeTexts,
)
from os.path import dirname, join
from schemas.MilvusSchemas import DeliriumNetworkSchema


class DeliriumnetworkSpider(CrawlSpider):
    name = "deliriumnetwork"
    allowed_domains = ["deliriumnetwork.org"]
    collection_name = "deliriumnetwork"
    schema = DeliriumNetworkSchema()
    transformer_name = "all-MiniLM-L6-v2"
    PDFDIR = join(dirname(dirname(__file__)), "pdf", name)
    custom_settings = {
        "ITEM_PIPELINES": {
            DownloadPubMedPDF: 100,
            ParsePDFFromFiles: 101,
            EncodeTexts: 102,
            MilvusStore: 103,
        }
    }

    def start_requests(self):
        for page in range(
            int(self.start_page), int(self.start_page) + int(self.page_num)
        ):
            self.logger.info(f"crawling page {page}")
            params = urlencode(dict(listpage=page, instance=2))
            yield scrapy.Request(
                url=f"https://deliriumnetwork.org/bibliography/?{params}",
                callback=self.get_record_details,
            )

    def get_record_details(self, response):
        links = LinkExtractor(
            allow=(r"\?pdb=\d+"), restrict_css="table.pages tbody tr", unique=True
        ).extract_links(response)
        for link in links:
            yield scrapy.Request(url=link.url, callback=self.parse_item)

    def parse_item(self, response):
        iL = ItemLoader(item=Information(), selector=response.css("#pdb-main"))
        iL.add_css("title", "dd.first_name::text")
        iL.add_css("authors", "dd.last_name::text")
        iL.add_css("year", "dd.address::text")
        iL.add_css("id", "dd.country a::attr(href)", re=r"[^\/]+$")
        iL.add_value("file_urls", iL.get_output_value("id"))
        iL.add_css("keywords", "dd.zip p::text")
        item = iL.load_item()
        return item
