from bs4 import BeautifulSoup
from datetime import datetime


def GetContent(parsedHTML):
    '''Returns html tags content'''
    return parsedHTML.get_text(separator='\n', strip=True)


def GetWebpageHeaders(parsedHTML):
    '''Returns html headers tags content'''
    headers = {}
    for i in range(1, 7):
        targetHeaders = 'h' + str(i)
        headers[targetHeaders] = []
        for header in parsedHTML.find_all(targetHeaders):
            headers[targetHeaders].append(GetContent(header))

        if not headers[targetHeaders]:
            del headers[targetHeaders]

    return headers


def GetWebpageMetaTags(parsedHTML, targetMetaTags):
    '''Returns html meta tag information'''
    metaTags = {}
    for metaTag in parsedHTML.find_all('meta'):
        if (
            metaTag.get('name')
            and metaTag.get('name').lower() in targetMetaTags
            and metaTag.get('content')
        ):
            metaTags[metaTag.get('name').lower()] = metaTag.get('content')

    # store keywords as list of strings
    if 'keywords' in metaTags:
        metaTags['keywords'] = [
            keyword.strip() for keyword in metaTags['keywords'].split(',')
        ]

    return metaTags


def GetWebpageLinks(parsedHTML):
    '''Returns urls starting with https:// or http:// extracted from href attributes of html a tags'''
    links = []
    for link in parsedHTML.find_all('a'):
        if link.get('href') and (link.get('href').startswith('https://') or link.get('href').startswith('http://')):
            links.append(link.get('href'))

    return links


def GetWebpageInformation(html, getLinks, targetMetaTags):
    '''Returns information of an html file'''
    parsed = BeautifulSoup(html, 'lxml')

    # Remove script and style tags
    for tag in parsed(['script', 'style']):
        tag.decompose()

    # Return page information
    return {
        'title': parsed.title.string if parsed.title else '',
        'meta': GetWebpageMetaTags(parsed, targetMetaTags),
        'content': GetContent(parsed),
        'headers': GetWebpageHeaders(parsed),
        'lastVisited': datetime.now(),
        'links': GetWebpageLinks(parsed) if getLinks else []
    }
