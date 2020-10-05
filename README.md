# wikipedia crawler:

I'm crawling wikipedia website, and i want to store them in a database(postgresql maybe). My future plans, use this data base and make a full stack app.

**P.S: [docker](https://www.docker.com/) is required**

## installation:

1. clone the repo `git clone https://github.com/cs-fedy/wikipedia-crawler`
2. run `docker compose up -d` to start the db.
3. install virtualenv using pip: `sudo pip install virtualenv`
4. create a new virtualenv:  `virtualenv venv`
5. activate the virtualenv: `source venv/bin/activate`
6. install requirements: `pip install requirements.txt`
7. run the script and enjoy: `python scraper.py`

## used tools:

1. [requests](https://pypi.org/project/requests/): Python HTTP for Humans.
2. [BeautifulSoup](https://pypi.org/project/beautifulsoup4/): Beautiful Soup is a library that makes it easy to scrape information from web pages. It sits atop an HTML or XML parser, providing Pythonic idioms for iterating, searching, and modifying the parse tree.
3. [python-dotenv](https://pypi.org/project/python-dotenv/): Add .env support to your django/flask apps in development and deployments.
4. [psycopg2](https://pypi.org/project/psycopg2/): psycopg2 - Python-PostgreSQL Database Adapter.
5. [tabulate](https://pypi.org/project/tabulate/): Pretty-print tabular data.

## Author:
**created at 🌙 with 💻 and ❤ by f0ody**
* **Fedi abdouli** - **wikipedia crawler** - [fedi abdouli](https://github.com/cs-fedy)
* my twitter account [FediAbdouli](https://www.twitter.com/FediAbdouli)
* my instagram account [f0odyy](https://www.instagram.com/f0odyy)
