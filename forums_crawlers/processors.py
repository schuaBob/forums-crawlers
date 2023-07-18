from typing import Any
from Bio import Entrez
import os


def to_int(s: str) -> int:
    return int(s.replace(",", ""))


def is_reply(comments: list) -> list:
    res = [False]
    if len(comments) > 0:
        res.extend([True for _ in comments])
    return res


class PMIDtoPDFUrl:
    def __init__(self):
        Entrez.email = os.environ.get("ENTREZ_EMAIL")
        Entrez.api_key = os.environ.get("ENTREZ_API_KEY")

    def __call__(self, pmid) -> str:
        url = ""
        handle = Entrez.elink(id=pmid, cmd="prlinks")
        for obj in Entrez.read(handle)[0]["IdUrlList"]["IdUrlSet"][0]["ObjUrl"]:
            if obj["Provider"]["NameAbbr"] == "PMC":
                url = obj["Url"] + "pdf"
                break
        handle.close()
        return [url]
