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

def format_body(data): # gonna need a lot of sanitization for this one....
    data = data.replace("\n", " NEWLINECHAR ").replace("\r", " NEWLINECHAR ").replace('"', "'")
    return data

def transaction_builder(sql):
    global sql_transaction
    sql_transaction.append(sql)
    if len(sql_transaction) > 1000:
        c.execute('BEGIN TRANSACTION')
        for s in sql_transaction:
            try:
                c.execute(s)
            except Exception as e:
                pass
        conn.commit()
        sql_transaction = []


def sql_update_replace_comment(comment_id, parent_id, parent_data, body, subreddit, created_utc, score):
    try:
        sql = """UPDATE parent_reply SET parent_id = {}, comment_id = {}, parent = {}, comment = {}, subreddit = {}, unix = {}, score = {} WHERE parent_id ={};""".format(parent_id, comment_id, parent_data, body, subreddit, int(created_utc), score, parent_id)
        transaction_builder(sql)
    except Exception as e:
        print("EXCEPTION ==> method sql_update_replace_comment failed: {}".format(e))
        return

def sql_insert_has_parent(comment_id, parent_id, parent_data, body, subreddit, created_utc, score):
    try:
        sql = """INSERT INTO parent_reply (parent_id, comment_id, parent, comment, subreddit, unix, score) VALUES ("{}","{}","{}","{}","{}","{}","{}");""".format(parent_id, comment_id, parent_data, body, subreddit, int(created_utc), score)
        transaction_builder(sql)
    except Exception as e:
        print("EXCEPTION ==> method sql_insert_has_parent failed: {}".format(e))
        return

def sql_insert_no_parent(comment_id, parent_id, body, subreddit, created_utc, score):
    try:
        sql = """INSERT INTO parent_reply (parent_id, comment_id, comment, subreddit, unix, score) VALUES ("{}","{}","{}","{}","{}","{}");""".format(parent_id, comment_id, body, subreddit, int(created_utc), score)
        transaction_builder(sql)
    except Exception as e:
        print("EXCEPTION ==> method sql_insert_no_parent failed: {}".format(e))
        return

def find_parent(data_id):
    try:
        c.execute("SELECT comment FROM parent_reply WHERE comment_id = '{}' LIMIT 1".format(data_id))
        result = c.fetchone()
        if result != None:
            #print (result)
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
        if result != None:
            #print (result)
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
            created_utc = row['created_utc']
            subreddit = row['subreddit']
            score = row['score']
            comment_id = row['name']
            parent_data = find_parent(parent_id)

            if score >= 2:
                # Going to maintain a 1 to 1 relationship for now
                # Check if parent already has a child, if so what's
                # the score of the child
                if acceptable(body):
                    existing_comment_score = find_existing_score(parent_id)
                    if existing_comment_score:
                        if score > existing_comment_score:
                            # We going for wholesome comments here ;)
                            # If condition is satisfied, update
                            sql_update_replace_comment(comment_id, parent_id, parent_data, body, subreddit, created_utc, score)

                    else:
                        if parent_data:
                            sql_insert_has_parent(comment_id, parent_id, parent_data, body, subreddit, created_utc, score)
                            paired_rows += 1
                        else:
                            sql_insert_no_parent(comment_id, parent_id, body, subreddit, created_utc, score)

            if row_counter % 100000 == 0:
                print("Total rows read: {}, Paired rows: {}, Time: {}".format(row_counter, paired_rows, str(datetime.now())))
