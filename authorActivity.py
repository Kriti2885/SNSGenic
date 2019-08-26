"""
@Author: Kriti Upadhyaya

@Order of Execution: 7

@Dependencies
pip install mysql
pip install praw

The programme extracts comments from Reddit for the list of submission id's given.
In Reddit, the submissions and comments are stored in a tree manner.


"""

import mysql.connector
import praw
import time
from praw.models import MoreComments


def mysql_db():
    """
    The function creates database connection and call function createTable for table creation.

    :return: None
    """
    myDB = mysql.connector.connect(host="HOST_NAME", user="DATABASE_USER",
                                   passwd="DATABASE_PASSWORD",
                                   database="DATABASE_NAME")

    myCursor = myDB.cursor()
    createTables(myCursor)
    read_text(myCursor, myDB)

    myCursor.close()
    myDB.close()


def createTables(myCursor):
    """
    This function creates table commentData in the database.

    :param myCursor: SQL Cursor to execute sql query.
    :return: None
    """

    myCursor.execute("CREATE TABLE `commentData` "
                     "(`comment_id` varchar(20) NOT NULL,`subreddit` varchar(200) "
                     "DEFAULT NULL,`submission_url` mediumtext,`parent` varchar(50), "
                     "`body` longtext CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci"
                     ",`date` datetime NOT NULL,`No_of_comment` int(11) DEFAULT NULL,"
                     "`score` int(11) DEFAULT NULL,`author` varchar(50) NOT NULL,`polarity`"
                     " double,`subjectivity` double,`personal_pr` double, `possesive_pr` "
                     "double,`adverb` double,`verb` double,`noun` double,`readability` double ,`related`"
                     " char(3),`category` varchar(10) NOT NULL,`first_person` int(11),`second_person` int(11),"
                     "`third_person` int(11),`word_count` int(11),`experientiality` float, `emotional_keyword` int(11) "
                     ",`social_life_keyword` int(11),`temporal_keyword` int(11),`worklife_keyword` int(11),"
                     "`cognitive_keyword` int(11),`inhibition_keyword` int(11),`socialmedia_keyword` int(11) "
                     ",PRIMARY KEY(`comment_id`,`date`)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 "
                     "COLLATE=utf8mb4_unicode_ci;")


def read_text(myCursor, myDB):
    """
    Select query selects submission id from database.
    The function then calls dictAuthor function which pulls data from Reddit.

    :param my_cursor:
    :return: None
    """
    myCursor.execute("select submission_id category from postData")
    authorList = myCursor.fetchall()
    dictAuthor(authorList, myCursor, myDB)


def createRedditObject():
    """
    This function creates a redditObject using function calls in Python Wrapper praw.

    Prior to creating an object, do the following:
    1. Reddit Account -	A Reddit account is required to access Reddits API.
    2. Client ID & Client Secret- These two values are needed to access Reddit API as
        a script application . If you dont already have a client ID and client secret,
        follow Reddit First Steps Guide to create them.
    3. User Agent-	A user agent is a unique identifier that helps Reddit determine the
       source of network requests. To use Reddits API, you need a unique and descriptive user agent.

    :return: redditObject
    """
    redditObject = praw.Reddit(client_id='CLIENT_ID', client_secret='CLIENT_SECRET',
                               user_agent='USER_AGENT')
    return redditObject


def dictAuthor(submissionList, myCursor, myDB):
    """
    Method calls createRedditObject() function which returns a reddit object.
    For all the submission id's, program extracts comment and is pushed to the database
    table commentData through database cursor.

    :param myCursor: SQL Cursor to execute sql query.
    :param authorList : List of submission id's wxtracte from postData table
    :return: None
    """

    commentID = []
    for i in range(0, len(submissionList)):
        submissionID = submissionList[i][0]
        category = submissionList[i][1]
        print("submission: " + submissionID)

        redditObject = createRedditObject()

        submission = redditObject.submission(submissionID)

        for top_level_comment in submission.comments:
            if isinstance(top_level_comment, MoreComments):
                continue
            body = str(top_level_comment.body)
            id = str(top_level_comment.id)
            date = correct_date(top_level_comment.created_utc)
            author = str(top_level_comment.author)
            parent = str(top_level_comment.parent_id)
            subreddit = str(top_level_comment.subreddit)
            score = top_level_comment.score

            sql = "Insert into commentData(comment_id, author, body, date, parent, subreddit, score, " \
                  "category) values(%s, %s, %s, %s, %s, %s, %s, %s)"
            val = (id, author, body, date, parent, subreddit, score, category)
            print id, author, body, date, parent, subreddit, score, category

            myCursor.execute(sql, val)

        myDB.commit()


def correct_date(date):
    """
    The function is used to convert epoch time to y/m/d hh:mm:ss format. The date field extracted
    from reddit is in epoch format.

    :param date:
    :return: time
    """
    fmt = "%Y-%m-%d %H:%M:%S"
    t = time.strftime(fmt, time.localtime(date))
    return t


if __name__ == "__main__":
    mysql_db()