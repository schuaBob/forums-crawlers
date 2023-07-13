import scrapy

from scrapy.linkextractors import LinkExtractor
from scrapy.spiders import CrawlSpider
from urllib.parse import urlencode
from scrapy.loader import ItemLoader
from forums_crawlers.items import Information
from forums_crawlers.pipelines import DownloadPubMedPDF, ParsePDF, MilvusStore, DownloadPubMedPDF2
import logging
from os.path import dirname, join

class DeliriumnetworkSpider(CrawlSpider):
    name = "deliriumnetwork"
    allowed_domains = ["deliriumnetwork.org"]
    collection_name = "deliriumnetwork"
    PDFDIR = join(dirname(dirname(__file__)), "pdf", name)
    custom_settings = {
        "ITEM_PIPELINES": {
            DownloadPubMedPDF: 100,
            # ParsePDF:101,
            # MilvusStore: 102,
        }
    }

    def start_requests(self):
        for page in range(
            int(self.start_page), int(self.start_page) + int(self.page_num)
        ):
            params = urlencode(dict(listpage=page, instance=2))
            yield scrapy.Request(
                url=f"https://deliriumnetwork.org/bibliography/?{params}",
                callback=self.get_record_details,
            )

    def get_record_details(self, response):
        links = LinkExtractor(
            allow=(r"\?pdb=\d+"), restrict_css="table.pages tbody tr", unique=True
        ).extract_links(response)
        self.log(f"Links: {len(links)}, {response.url}", logging.INFO)
        for link in links:
            yield scrapy.Request(url=link.url, callback=self.parse_item)

    def parse_item(self, response):
        iL = ItemLoader(item=Information(), selector=response.css("#pdb-main"))
        iL.add_css("title", "dd.first_name::text")
        iL.add_css("authors", "dd.last_name::text")
        iL.add_css("year", "dd.address::text")
        iL.add_css("pmid", "dd.country a::text", re=r"^(?![a-zA-Z])\s*\d+")
        iL.add_value("url", iL.get_output_value("pmid"))
        iL.add_css("keywords", "dd.zip p::text")
        return iL.load_item()
