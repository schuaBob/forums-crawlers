import scrapy
from scrapy.linkextractors import LinkExtractor
from scrapy.spiders import CrawlSpider
from scrapy.loader import ItemLoader
from forums_crawlers.items import Discussion


class AlzconnectedSpider(CrawlSpider):
    name = "alzconnected"
    allowed_domains = ["alzconnected.org"]
    collection_name = "AlzConnected-v2"

    def start_requests(self):
        for page in range(
            int(self.start_page), int(self.start_page) + int(self.page_num)
        ):
            yield scrapy.Request(
                url=f"https://alzconnected.org/categories/{self.category}/p{page}",
                callback=self.get_discussions,
            )

    def get_discussions(self, response):
        linkExtractor = LinkExtractor(
            allow=(r"discussion/\d+/\S+",),
            restrict_css="section.MainContent ul li",
            unique=True,
        )
        for link in linkExtractor.extract_links(response):
            yield scrapy.Request(url=link.url, callback=self.parse_discussion)

    def parse_discussion(self, response):
        iL = ItemLoader(item=Discussion(), selector=response.css("section.MainContent"))
        iL.add_css("title", ".PageTitle h1::text")
        iL.add_value("post_id", response.url, re=r"discussion/(\d+)/")
        iL.add_value("url", response.url)
        iL.add_css("datetimes", ".MItem.DateCreated time::attr(datetime)")
        iL.add_css("user_ids", "span.Author a.Username::attr(data-userid)")
        iL.add_css("user_post_count", ".MItem.PostCount b::text")
        iL.add_css("bodys", ".Item-Body .Message.userContent")
        iL.add_css("replies", "ul.Comments > li")
        return iL.load_item()
