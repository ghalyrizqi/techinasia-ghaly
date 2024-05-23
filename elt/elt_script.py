import re
import requests
import pandas as pd
from sqlalchemy import create_engine
import sqlalchemy
import json
from bs4 import BeautifulSoup


db_host = 'tia_postgres'
db_port = '5432'  # Internal Docker port, not the mapped one
db_name = 'tia_postgres_db'
db_user = 'postgres'
db_password = 'secret'
table_name = 'posts'


def clean_html_and_count_words(content):
    """ Clean HTML tags and count words in the provided content.
    Args:
        content (str): The content string to be processed.
    Returns:
        int: The number of words in the cleaned content.
    """
    if pd.isna(content):
        return 0

    # Use BeautifulSoup to remove HTML tags
    soup = BeautifulSoup(content, "html.parser")

    # Get the text from the entire HTML body
    if soup.body:
        text = soup.body.get_text(separator=' ')
    else:
        # If there is no <body> tag, use the entire HTML content
        text = soup.get_text(separator=' ')

    # Split on whitespace to count words
    words = text.split()
    return len(words)


def fetch_and_insert_data(db_user, db_password, db_host, db_port, db_name, table_name):
    """
    Fetch data from the API, preprocess it, and insert it into the specified database table.

    Args:
        db_user (str): The database user name.
        db_password (str): The database password.
        db_host (str): The database host.
        db_port (str): The database port.
        db_name (str): The database name.
        table_name (str): The name of the table to insert data into.

    Returns:
        None
    """
    # Construct the database connection URL
    db_url = f'postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}'

    # Create a database engine
    engine = create_engine(db_url)

    # Create the table if it doesn't exist
    create_table_query = f"""
    CREATE TABLE IF NOT EXISTS public.{table_name} (
        id varchar(50) NOT NULL,
        date_gmt timestamp with time zone NOT NULL,
        modified_gmt timestamp with time zone NOT NULL,
        title text NOT NULL,
        slug varchar(255) NULL,
        status varchar(20) NOT NULL,
        "type" varchar(20) NOT NULL,
        link text NOT NULL,
        "content" text NULL,
        excerpt text NULL,
        author jsonb NULL,
        editor text NULL,
        comment_status varchar(20) NOT NULL,
        comments_count int4 DEFAULT 0 NOT NULL,
        "comments" jsonb NULL,
        featured_image jsonb NULL,
        post_images jsonb NULL,
        seo jsonb NULL,
        categories jsonb NULL,
        tags jsonb NULL,
        companies jsonb NULL,
        is_sponsored bool DEFAULT false NOT NULL,
        sponsor jsonb NULL,
        is_partnership bool DEFAULT false NOT NULL,
        external_scripts text NULL,
        show_ads bool DEFAULT true NOT NULL,
        is_subscriber_exclusive bool DEFAULT false NOT NULL,
        is_paywalled bool DEFAULT false NOT NULL,
        is_inappbrowser bool DEFAULT false NOT NULL,
        read_time int4 NOT NULL,
        word_count int DEFAULT 0 NOT NULL,
        CONSTRAINT {table_name}_pkey PRIMARY KEY (id),
        CONSTRAINT {table_name}_slug_key UNIQUE (slug)
    );
    """

    with engine.connect() as conn:
        conn.execute(sqlalchemy.text(create_table_query))

    # Fetch data from the API
    url = "https://www.techinasia.com/wp-json/techinasia/2.0/posts"
    headers = {
        'Cache-Control': 'no-cache',
        'User-Agent': 'PostmanRuntime/7.38.0',
        'Accept': '*/*',
        'Accept-Encoding': 'gzip, deflate, br',
        'Connection': 'keep-alive'
    }

    data = fetch_api_data(url, headers)

    # Create a DataFrame from the data
    df = pd.DataFrame(data)

    # Convert datetime strings to Python datetime objects
    df['date_gmt'] = pd.to_datetime(df['date_gmt'])
    df['modified_gmt'] = pd.to_datetime(df['modified_gmt'])

    # Compute word count
    df['word_count'] = df['content'].apply(clean_html_and_count_words)

    # Convert the DataFrame to a list of dictionaries
    records = df.to_dict('records')

    # Insert the data into the table
    insert_data(records, table_name, engine)


def fetch_api_data(url, headers):
    """
    Fetch data from the API and return a list of dictionaries.

    Args:
        url (str): The API URL.
        headers (dict): The headers for the API request.

    Returns:
        list: A list of dictionaries containing the API data.
    """
    data = []

    # Make the initial request
    response = requests.request("GET", url, headers=headers, json={})
    response_data = response.json()

    # Check if the 'posts' key exists in the initial response
    if 'posts' in response_data:
        data.extend(response_data['posts'])

    # Get the total number of pages
    total_pages = response_data.get('total_pages', 1)

    # Loop through the remaining pages
    page = 2
    while page <= total_pages:
        next_url = f"{url}?page={page}"

        response = requests.request("GET", next_url, headers=headers, json={})

        if response.ok:
            response_data = response.json()

            if 'posts' in response_data:
                data.extend(response_data['posts'])
            else:
                print(f"No 'posts' key found in the response for page {page}")
        else:
            error_data = response.json()
            if error_data.get('code') == 'rest_invalid_param' and error_data.get('data', {}).get('params', {}).get('page') == 'Invalid parameter.':
                print(
                    "Encountered 'Invalid parameter(s): page' error. Stopping the loop.")
                break
            else:
                print(f"Error fetching page {page}: {error_data}")

        page += 1

    return data


def insert_data(records, table_name, engine):
    """
    Insert data into the specified table using SQLAlchemy.

    Args:
        records (list): A list of dictionaries containing the data to be inserted.
        table_name (str): The name of the table to insert data into.
        engine (sqlalchemy.engine.base.Engine): The SQLAlchemy engine object.

    Returns:
        None
    """
    insert_query = f"""
    INSERT INTO {table_name} (
        id, date_gmt, modified_gmt, title, slug, status, type, link, content, excerpt,
        author, editor, comment_status, comments_count, comments, featured_image,
        post_images, seo, categories, tags, companies, is_sponsored, sponsor,
        is_partnership, external_scripts, show_ads, is_subscriber_exclusive,
        is_paywalled, is_inappbrowser, read_time, word_count
    ) VALUES (
        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
        %s, %s, %s, %s, %s, %s,
        %s, %s, %s, %s, %s, %s, %s,
        %s, %s, %s, %s, %s, %s, %s, %s
    )
    ON CONFLICT (id) DO UPDATE SET
        date_gmt = EXCLUDED.date_gmt,
        modified_gmt = EXCLUDED.modified_gmt,
        title = EXCLUDED.title,
        slug = EXCLUDED.slug,
        status = EXCLUDED.status,
        type = EXCLUDED.type,
        link = EXCLUDED.link,
        content = EXCLUDED.content,
        excerpt = EXCLUDED.excerpt,
        author = EXCLUDED.author,
        editor = EXCLUDED.editor,
        comment_status = EXCLUDED.comment_status,
        comments_count = EXCLUDED.comments_count,
        comments = EXCLUDED.comments,
        featured_image = EXCLUDED.featured_image,
        post_images = EXCLUDED.post_images,
        seo = EXCLUDED.seo,
        categories = EXCLUDED.categories,
        tags = EXCLUDED.tags,
        companies = EXCLUDED.companies,
        is_sponsored = EXCLUDED.is_sponsored,
        sponsor = EXCLUDED.sponsor,
        is_partnership = EXCLUDED.is_partnership,
        external_scripts = EXCLUDED.external_scripts,
        show_ads = EXCLUDED.show_ads,
        is_subscriber_exclusive = EXCLUDED.is_subscriber_exclusive,
        is_paywalled = EXCLUDED.is_paywalled,
        is_inappbrowser = EXCLUDED.is_inappbrowser,
        read_time = EXCLUDED.read_time,
        word_count = EXCLUDED.word_count
    """

    with engine.connect() as conn:
        for record in records:
            values = (
                record['id'], record['date_gmt'], record['modified_gmt'], record['title'], record['slug'], record['status'],
                record['type'], record['link'], record['content'], record['excerpt'], json.dumps(
                    record['author']), record['editor'],
                record['comment_status'], record['comments_count'], json.dumps(
                    record['comments']), json.dumps(record['featured_image']),
                json.dumps(record['post_images']), json.dumps(record['seo']), json.dumps(
                    record['categories']), json.dumps(record['tags']), json.dumps(record['companies']),
                record['is_sponsored'], json.dumps(
                    record['sponsor']), record['is_partnership'], record['external_scripts'],
                record['show_ads'], record['is_subscriber_exclusive'], record['is_paywalled'], record['is_inappbrowser'],
                record['read_time'], record['word_count']
            )
            conn.execute(insert_query, values)


fetch_and_insert_data(db_user, db_password, db_host,
                      db_port, db_name, table_name)
