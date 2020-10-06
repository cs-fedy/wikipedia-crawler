import requests
from bs4 import BeautifulSoup
import os
from dotenv import load_dotenv
import tabulate
import psycopg2
import re
import string
from collections import OrderedDict

load_dotenv()


# TODO: use regex and refactor the code


class DB:
    def __init__(self):
        self.__POSTGRES_DB = os.getenv("POSTGRES_DB")
        self.__POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")
        self.__POSTGRES_USER = os.getenv("POSTGRES_USER")
        self.connection = None
        self.cursor = None
        self.connect()
        self.drop_tables(["page", "link", "file"])
        self.create_tables()

    def connect(self):
        try:
            self.connection = psycopg2.connect(user=self.__POSTGRES_USER,
                                               password=self.__POSTGRES_PASSWORD,
                                               host="127.0.0.1",
                                               port="5432",
                                               database=self.__POSTGRES_DB)
            self.cursor = self.connection.cursor()
            print("connected to db successfully")
        except (Exception, psycopg2.Error) as error:
            print("failed to connect to db", error)

    def create_tables(self):
        if not self.connection:
            return

        queries = []
        # page(page_id_, page_url, added_in, content)
        page_table_query = """
            CREATE TABLE page(
                page_id SERIAL PRIMARY KEY,
                page_url TEXT,
                page_title TEXT,
                page_content TEXT,
                added_in TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP); 
        """
        queries.append((page_table_query, "page"))

        # link(link_id, page_id*, link, added_in)
        link_table_query = """
            CREATE TABLE link(
                link_id SERIAL PRIMARY KEY,
                page_id INT,
                link VARCHAR(255),
                added_in TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (page_id) REFERENCES page(page_id)); 
        """
        queries.append((link_table_query, "link"))

        # file(file_id, page_id*, file_url, added_in)
        file_table_query = """
            CREATE TABLE file(
                file_id SERIAL PRIMARY KEY,
                page_id INT,
                file_url VARCHAR(255),
                added_in TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (page_id) REFERENCES page(page_id)); 
        """
        queries.append((file_table_query, "file"))

        # * tables creation
        for query in queries:
            query_text, table_name = query
            self.cursor.execute(query_text)
            self.connection.commit()
            print(f"Table {table_name} created successfully in PostgreSQL ")

    def seed_page_table(self, record):
        if not self.connection:
            return

        title, page_url, first_paragraph, files = record.values()
        seeding_page_query = """ 
                INSERT INTO page (page_title, page_url, page_content)  
                VALUES (%s, %s, %s)
        """
        self.cursor.execute(seeding_page_query, (title, page_url, first_paragraph))
        page_id = self.cursor.fetchone()[0]
        self.connection.commit()

        # seed file table with files
        self.seed_file_table(page_id, files)
        print(f"seeding page table with {title} details done")
        return page_id

    def seed_link_table(self, page_id, link):
        if not self.connection:
            return

        seeding_link_query = """ 
                INSERT INTO link (page_id, link)  
                VALUES (%d, %s)
        """
        self.cursor.execute(seeding_link_query, (page_id, link))
        self.connection.commit()
        print(f"seeding link table with {link}")

    def seed_file_table(self, page_id, file):
        if not self.connection:
            return

        seeding_file_query = """ 
                INSERT INTO file (page_id, link)  
                VALUES (%d, %s)
        """
        self.cursor.execute(seeding_file_query, (page_id, file))
        self.connection.commit()
        print(f"seeding link file with {file}")

    def close_connection(self):
        if not self.connection:
            return

        self.cursor.close()
        self.connection.close()
        print("PostgreSQL connection is closed")

    def drop_tables(self, tables_names):
        for table_name in tables_names:
            drop_table_query = f"DROP TABLE IF EXISTS {table_name} CASCADE"
            self.cursor.execute(drop_table_query)
            self.connection.commit()
            print(f"table {table_name} dropped")

    def get_data(self, table_name):
        if not self.connection:
            return

        row_select_query = f"SELECT * FROM {table_name}"
        self.cursor.execute(row_select_query)
        rows = [row[0] for row in self.cursor.fetchall()]
        columns_select_query = f"SELECT column_name FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '{table_name}';"
        self.cursor.execute(columns_select_query)
        columns = [col[0] for col in self.cursor.fetchall()]
        print("=" * 28, f"@ rows in {table_name} table @", "=" * 28)
        print(tabulate.tabulate(rows, headers=columns, tablefmt="psql"))
        print("\n")


class ScrapeWikiData:
    def __init__(self, source_code):
        self.source_code = source_code

    @staticmethod
    def __clean_content(content):
        filtered_text = re.sub("\n+", " ", content)
        filtered_text = re.sub(" +", " ", filtered_text)
        filtered_text = re.sub("\[[0-9]*]", "", filtered_text)
        filtered_text = bytes(filtered_text, "UTF-8")
        filtered_text = filtered_text.decode("ascii", "ignore")
        filtered_text = filtered_text.strip()
        return [
            item.strip(string.punctuation)
            for item in filtered_text.split()
            if len(item) > 1 and item.lower() not in ["i", "a"]
        ]

    def __get_ngrams(self, content, n: int):
        text_words = self.__clean_content(content)
        ngrams = []
        for index in range(len(text_words) - n + 1):
            new_ngram = text_words[index:index + n]
            if new_ngram not in ngrams:
                ngrams.append(new_ngram)
        return ngrams

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
            "first_paragraph": self.__get_ngrams(paragraph, 2),
            "files": files
        }


class WikiCrawler:
    def __init__(self, page_url):
        self.page_url = page_url
        self.csv_file_path = r"assets/data.csv"
        self.internal_link = set()
        self.external_link = set()
        self.recursion_limit = 1
        # DB.__init__(self)
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
        print(scraped_data["first_paragraph"])
        # page_id = self.seed_page_table(scraped_data)
        # print(f"@@@ scraping {page_url} done")
        # for link in soup.select("#mw-content-text .mw-parser-output a"):
        #     if "href" not in link.attrs:
        #         continue
        #     elif ":" in link["href"] or "#" in link["href"]:
        #         continue
        #
        #     if link["href"].startswith("/wiki/"):
        #         new_article_url = f"https://en.wikipedia.org{link['href']}"
        #         if new_article_url not in self.internal_link:
        #             self.internal_link.add(new_article_url)
        #             self.seed_link_table(page_id, new_article_url)
        #             if recursion_depth < self.recursion_limit:
        #                 self.__get_urls(new_article_url, recursion_depth + 1)
        #     elif link["href"].startswith("http://") or link["href"].startswith("https://"):
        #         self.external_link.add(link["href"])


if __name__ == "__main__":
    url = "https://en.wikipedia.org/wiki/Food"
    wc = WikiCrawler(url)
