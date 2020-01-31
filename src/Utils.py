from bs4 import BeautifulSoup
from datetime import datetime
from urllib.parse import urlparse


def IsBaseDomain(url):
    '''Tells whether a url is a base domain or not'''
    path = urlparse(url).path
    return path == '' or path == '/'


def GetBaseDomain(url):
    '''Returns the base domain of an url'''
    result = urlparse(url)
    return result.scheme + '://' + result.netloc


def GetContent(parsedHTML):
    '''Returns html tags content'''

    if parsedHTML.body:
        return parsedHTML.body.get_text(separator=' ', strip=True)
    else:
        return parsedHTML.get_text(separator=' ', strip=True)


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
    metaTags = {key: None for key in targetMetaTags}
    for metaTag in parsedHTML.find_all('meta'):
        if (
            metaTag.get('name')
            and metaTag.get('name').lower() in targetMetaTags
            and metaTag.get('content')
        ):
            metaTags[metaTag.get('name').lower()] = metaTag.get('content')

    # store keywords as list of strings
    if 'keywords' in metaTags and metaTags['keywords'] != None:
        metaTags['keywords'] = [
            keyword.strip() for keyword in metaTags['keywords'].split(',')
        ]

    return metaTags


def GetWebpageLinks(parsedHTML, url, all=False):
    '''Returns urls starting with https:// or http:// extracted from href attributes of html a tags'''
    links = []
    for link in parsedHTML.find_all('a'):
        if link.get('href') and (link.get('href').startswith('https://') or link.get('href').startswith('http://')):
            if not all and GetBaseDomain(url) != GetBaseDomain(link.get('href')):
                continue

            links.append(link.get('href'))

    return links


def GetWebpageInformation(url, html, getLinks, targetMetaTags, allLinks):
    '''Returns information of an html file'''
    parsed = BeautifulSoup(html, 'lxml')

    # Remove script and style tags
    for tag in parsed(['script', 'style']):
        tag.decompose()

    # Extract and remove headers
    headers = GetWebpageHeaders(parsed)
    for tag in parsed(['h' + str(i) for i in range(1, 7)]):
        tag.decompose()

    # Return page information
    return {
        'title': parsed.title.string if parsed.title else '',
        'meta': GetWebpageMetaTags(parsed, targetMetaTags),
        'content': GetContent(parsed),
        'headers': headers,
        'lastVisited': datetime.now(),
        'links': GetWebpageLinks(parsed, url, allLinks) if getLinks else []
    }
