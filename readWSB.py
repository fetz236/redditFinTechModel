from psaw import PushshiftAPI
import datetime as dt
import psycopg2
import psycopg2.extras

connection = psycopg2.connect(host=config.DB_HOST, database=config.DB_NAME, user=config.DB_USER, password=config.DB_PASS)
cursor = connection.cursor(cursor_factory=psycopg2.extras.DictCursor)
cursor.execute("""
    SELECT * FROM stock;
""")
rows = cursor.fetchall()
stocks = {}
for row in rows:
    stocks['$' + row['symbol']] = row['id']

print(stocks)
api = PushshiftAPI()

start_time =int(dt.datetime(2021, 6, 8).timestamp())

#gets submissions limits to 10
posts = api.search_submissions(after=start_time,
                               subreddit='wallstreetbets',
                               filter=['url','author', 'title', 'subreddit'],
                               limit=10)


for post in posts:
    words = post.title.split()
    cashtags = list(set(filter(lambda word: word.lower().startswith('$'), words)))
    
    if len(cashtags)>0:
        try:

            post_time = dt.datetime.fromtimestamp(post.created_utc).isoformat()
            cursor.execute("""
                INSERT INTO mention (dt, stock_id, message, source, url)
                VALUES (%s, %s, %s, 'wallstreetbets', %s)
            """, (post_time, stocks[cashtag], post.title, post.url))

            connection.commit()
        
        except Exception as e:
            print(e)
            connection.rollback()
