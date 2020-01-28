from pymongo import MongoClient
import urllib.request
from bs4 import BeautifulSoup
from datetime import datetime

mongoClient = MongoClient(port=27017)

pagesDB = mongoClient.pagesDB

pagesDB.pending.delete_many({})
pagesDB.registered.delete_many({})

result = pagesDB.pending.insert_many(
    [
        {"url": "https://sport.es", "depth": 0},
        {"url": "https://mundodeportivo.com", "depth": 0},
        {"url": "https://marca.com", "depth": 0},
        {"url": "https://as.com", "depth": 0},
        # {"url": "https://sajfdkashdfkashdfklasdfhaskdf.com/"},
    ]
)


def urlIsRegistered(url):
    return pagesDB.registered.count_documents({"url": url}) > 0


def urlIsPending(url):
    return pagesDB.pending.count_documents({"url": url}) > 0


def GetPageLinks(parsedHTML):
    links = []
    for link in parsedHTML.find_all("a"):
        if link.get("href"):
            links.append(link.get("href"))

    return links


def GetContent(parsedHTML):
    return parsedHTML.get_text(separator="\n", strip=True)


def GetPageHeaders(parsedHTML):
    headers = {}
    for i in range(1, 7):
        targetHeaders = "h" + str(i)
        headers[targetHeaders] = []
        for header in parsedHTML.find_all(targetHeaders):
            headers[targetHeaders].append(GetContent(header))

        if not headers[targetHeaders]:
            del headers[targetHeaders]

    return headers


targetMetaTags = [
    "keywords",
    "description",
    "author",
    "lang",
    "locality",
    "organization",
]


def GetPageMetaTags(parsedHTML):
    # look for target meta tags
    metaTags = {}
    for metaTag in parsedHTML.find_all("meta"):
        if (
            metaTag.get("name")
            and metaTag.get("name").lower() in targetMetaTags
            and metaTag.get("content")
        ):
            metaTags[metaTag.get("name").lower()] = metaTag.get("content")

    # store keywords as list of strings
    if "keywords" in metaTags:
        metaTags["keywords"] = [
            keyword.strip() for keyword in metaTags["keywords"].split(",")
        ]

    return metaTags


def AddPagesToPending(urls, depth):

    urlDocuments = []
    for url in urls:
        if not urlIsPending(url) and not urlIsRegistered(url):
            urlDocuments.append({"url": url, "depth": depth})

    pagesDB.pending.insert_many(urlDocuments)


def RegisterPage(url, html, currentDepth):
    parsed = BeautifulSoup(html, "lxml")

    # Remove script and style tags
    for tag in parsed(["script", "style"]):
        tag.decompose()

    AddPagesToPending(GetPageLinks(parsed), currentDepth + 1)

    pagesDB.registered.insert_one(
        {
            "url": url,
            "title": parsed.title.string,
            "meta": GetPageMetaTags(parsed),
            "content": GetContent(parsed),
            "headers": GetPageHeaders(parsed),
            "lastVisited": datetime.now(),
        }
    )


def VisitURLS():

    for pendingURL in pagesDB.pending.find():
        url = pendingURL["url"]
        depth = pendingURL["depth"]
        print(url, depth)

        page = urllib.request.urlopen(url)
        statusCode = page.getcode()
        contentType = page.info()["Content-Type"]
        charset = page.headers.get_content_charset()
        if "text/html" in contentType:

            print(charset)
            html = page.read().decode(charset if charset else "utf8")
            RegisterPage(url, html, depth)


VisitURLS()
