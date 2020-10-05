import requests
from bs4 import BeautifulSoup


# TODO: use regex and refactor the code
# TODO: use postgresql db to store scraped data
# TODO: use auto-increment id and timestamps while inserting records


class ScrapeWikiData:
    def __init__(self, source_code):
        self.source_code = source_code

    def __call__(self, page_url):
        title = self.source_code.find("h1").getText()
        body = self.source_code.select_one("#mw-content-text .mw-parser-output")
        first_paragraph = [p for p in body.select(".mw-parser-output p")]
        if len(first_paragraph) > 0:
            paragraph = first_paragraph[1].getText()
        else:
            paragraph = "undefined"
        # TODO: check file extension: accept it or no
        files = [f"https:{file['src']}"
                 for file in body.findAll(src=True)
                 if file["src"].lower().find("icon") < 0]

        return {
            "title": title.strip(),
            "page_url": page_url,
            "first_paragraph": paragraph.strip(),
            "files": files
        }


class WikiCrawler:
    def __init__(self, page_url):
        self.page_url = page_url
        self.csv_file_path = r"assets/data.csv"
        self.internal_link = set()
        self.external_link = set()
        self.recursion_limit = 500
        self.__get_urls()

    @staticmethod
    def __request_data(page_url):
        # TODO: handle redirect(server and client side) on requesting a web page
        response = requests.get(page_url)
        if response.status_code == 200:
            return response.content
        raise Exception("error while extracting data", response.status_code)

    def __get_urls(self, page_url=None, recursion_depth=0):
        if not page_url:
            page_url = self.page_url
            self.internal_link.add(self.page_url)
        data = self.__request_data(page_url)
        soup = BeautifulSoup(data, "html.parser")
        # collect data from current page
        sdw = ScrapeWikiData(soup)
        scraped_data = sdw(page_url)
        print(scraped_data)
        print(f"@@@ scraping {page_url} done")
        for link in soup.select("#mw-content-text .mw-parser-output a"):
            if "href" not in link.attrs:
                continue
            elif ":" in link["href"] or "#" in link["href"]:
                continue

            if link["href"].startswith("/wiki/"):
                new_article_url = f"https://en.wikipedia.org{link['href']}"
                if new_article_url not in self.internal_link:
                    self.internal_link.add(new_article_url)
                    if recursion_depth < self.recursion_limit:
                        self.__get_urls(new_article_url, recursion_depth + 1)
            elif link["href"].startswith("http://") or link["href"].startswith("https://"):
                self.external_link.add(link["href"])


if __name__ == "__main__":
    url = "https://en.wikipedia.org/wiki/Food"
    wc = WikiCrawler(url)
