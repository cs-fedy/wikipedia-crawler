import requests
from bs4 import BeautifulSoup
import os
from dotenv import load_dotenv
import tabulate
import psycopg2
import re

load_dotenv()


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
            raise Exception(f"failed to connect to db {error}")

    def create_tables(self):
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

        # link(link_id, page_id*, link, added_in, language)
        link_table_query = """
            CREATE TABLE link(
                link_id SERIAL PRIMARY KEY,
                page_id INT,
                link TEXT,
                language TEXT,
                added_in TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (page_id) REFERENCES page(page_id)); 
        """
        queries.append((link_table_query, "link"))

        # file(file_id, page_id*, file_url, added_in)
        file_table_query = """
            CREATE TABLE file(
                file_id SERIAL PRIMARY KEY,
                page_id INT,
                file_url TEXT,
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
        title, page_url, first_paragraph, files = record.values()
        seeding_page_query = """ 
                INSERT INTO page (page_title, page_url, page_content)  
                VALUES (%s, %s, %s)
                returning page_id
        """
        self.cursor.execute(seeding_page_query, (title, page_url, first_paragraph))
        page_id = self.cursor.fetchone()[0]
        self.connection.commit()

        # seed file table with files
        self.seed_file_table(page_id, files)
        print(f"seeding page table with {title} details done")
        return page_id

    def seed_link_table(self, page_id, link, language):
        seeding_link_query = """ 
                INSERT INTO link (page_id, link, language)  
                VALUES (%s, %s, %s)
        """
        self.cursor.execute(seeding_link_query, (page_id, link, language))
        self.connection.commit()
        print(f"seeding link table with {link}")

    def seed_file_table(self, page_id, files):
        for file in files:
            get_files_query = f"SELECT file_url from file where file_url == {file}"
            self.cursor.execute(get_files_query)
            if len(self.cursor.fetchall()) > 0:
                continue

            seeding_file_query = """ 
                    INSERT INTO file (page_id, file_url)  
                    VALUES (%s, %s)
            """
            self.cursor.execute(seeding_file_query, (page_id, file))
            self.connection.commit()
            print(f"seeding file table with {file}")

    def close_connection(self):
        self.cursor.close()
        self.connection.close()
        print("PostgreSQL connection is closed")

    def drop_tables(self, tables_names):
        for table_name in tables_names:
            drop_table_query = f"DROP TABLE IF EXISTS {table_name} CASCADE"
            self.cursor.execute(drop_table_query)
            self.connection.commit()
            print(f"table {table_name} dropped")

    def __get_rows(self, table_name):
        row_select_query = f"SELECT * FROM {table_name}"
        self.cursor.execute(row_select_query)
        return [row for row in self.cursor.fetchall()]

    def __get_columns(self, table_name):
        columns_select_query = f"SELECT column_name FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '{table_name}';"
        self.cursor.execute(columns_select_query)
        return [col[0] for col in self.cursor.fetchall()]

    def show_data(self, table_name):
        rows = self.__get_rows(table_name)
        columns = self.__get_columns(table_name)
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
        return filtered_text.strip()

    def __call__(self, page_url):
        title = self.source_code.find("h1").getText()
        body = self.source_code.select_one("#mw-content-text .mw-parser-output")
        first_paragraph = [p for p in body.select(".mw-parser-output p") if p.getText().strip() != ""]
        if len(first_paragraph) > 0:
            paragraph = first_paragraph[0].getText()
        else:
            paragraph = "undefined"
        # TODO: check file extension: accept it or no
        files = [f"https:{file['src']}"
                 for file in body.findAll(src=True)
                 if file["src"].lower().find("icon") < 0]

        return {
            "title": title.strip(),
            "page_url": page_url,
            "first_paragraph": self.__clean_content(paragraph),
            "files": files
        }


class WikiCrawler(DB):
    def __init__(self, page_url):
        self.page_url = page_url
        self.csv_file_path = r"assets/data.csv"
        self.internal_link = set()
        self.external_link = set()
        self.recursion_limit = 1
        self.native_language = self.__get_article_language(self.page_url)
        DB.__init__(self)
        self.__get_urls()

    @staticmethod
    def __get_article_language(page_url):
        return page_url[page_url.find("//") + 2: page_url.find(".")]

    @staticmethod
    def __request_data(page_url):
        # TODO: handle redirect(server and client side) on requesting a web page
        # handling redirect: page 221: https://bit.ly/33K2OcG
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
        page_id = self.seed_page_table(scraped_data)
        print(f"@@@ scraping {page_url} done")

        # get translations urls
        if self.__get_article_language(page_url) == self.native_language:
            self.internal_link |= {link["href"] for link in soup.select("#p-lang a")}

        for link in soup.select("#mw-content-text .mw-parser-output a"):
            if "href" not in link.attrs:
                continue
            elif ":" in link["href"] or "#" in link["href"]:
                continue

            if link["href"].startswith("/wiki/"):
                new_article_url = f"https://en.wikipedia.org{link['href']}"
                if new_article_url not in self.internal_link:
                    self.internal_link.add(new_article_url)
            elif not link["href"].startswith("/w/"):
                self.external_link.add(link["href"][2:])

        for internal_url in self.internal_link:
            self.seed_link_table(page_id, url, self.__get_article_language(internal_url))
            if recursion_depth < self.recursion_limit:
                self.__get_urls(internal_url, recursion_depth + 1)


if __name__ == "__main__":
    url = "https://en.wikipedia.org/wiki/Food"
    wc = WikiCrawler(url)
