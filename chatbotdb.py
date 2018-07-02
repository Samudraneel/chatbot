import sqlite3
import json
from datetime import datetime

timeframe = '2012-10'
sql_transaction = []

conn = sqlite3.connect('{}.db'.format(timeframe))
c = conn.cursor()

def create_table():
    c.execute("""CREATE TABLE IF NOT EXISTS parent_reply (
                parent_id TEXT PRIMARY KEY,
                comment_id TEXT UNIQUE,
                parent TEXT,
                comment TEXT,
                subreddit TEXT,
                unix INTEGER,
                score INTEGER)""")
    return

def format_body(data):
    data = data.replace("\n", " NEWLINECHAR ").replace("\r", " NEWLINECHAR ").replace('"', "'")
    return data

def find_parent(data_id):
    try:
        c.execute("SELECT comment FROM parent_reply WHERE comment_id = '{}' LIMIT 1".format(data_id))
        result = c.fetchone()
        if result != None:
            print result
            return result[0] # Not sure what form so debug!!!
        else:
            return False
    except Exception as e:
        print ("EXCEPTION ==> method find_parent failed with exception: {}".format(e))
        return False

#TODO: refactor this method to one containing the one above and below
def find_existing_score(parent_id):
    try:
        c.execute("SELECT score FROM parent_reply WHERE parent_id = '{}' LIMIT 1".format(parent_id))
        result = c.fetchone()
        if result != None
            print result
            return result[0] # same as above function
        else:
            return False
    except Exception as e:
        print("EXCEPTION ==> method find_existing_score failed with exception: {}".format(e))
        return False

def acceptable(data):
    if len(data.split(' ')) > 50 or len(data) < 1:   # Check for words more than 50, or none
        return False
    elif len(data) > 1000:                           # No garbage (this is totally subjective btw)
        return False
    elif data == '[deleted]' or data == '[removed]': # Don't want 'no data'
        return False
    else:                                            # For now I'll take everything else. This is a base form that will change
        return True

if __name__ == "__main__":
    create_table()
    row_counter = 0
    paired_rows = 0
    with open("/home/modestybias/myProjs/data/reddit/RC_{}".format(timeframe), buffering=1000) as f:
        for row in f:
            #print(row)

            row_counter += 1
            row = json.loads(row)
            parent_id = row['parent_id']
            body = format_body(row['body'])
            utc = row['created_utc']
            subreddit = row['subreddit']

            parent_data = find_parent(parent_id)

            if score >= 2:
                # Going to maintain a 1 to 1 relationship for now
                # Check if parent already has a child, if so what's
                # the score of the child
                existing_comment_score = find_existing_score(parent_id)
                if existing_comment_score:
                    if score > existing_comment_score:
                        # We going for wholesome comments here ;)
                        # 
