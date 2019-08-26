"""
@Author : Kriti Upadhyaya

@Order of execution : 1st program file to be executed

@Dependencies to be installed -
    pip install praw
    pip install mysql

Code below is used to extract data from reddit using Reddit API and Python Wrapper PRAW.
The submission data is extracted from Reddit using a praw and then is pushed to database table.
"""


import praw
import time
import mysql.connector


def dbConnection():
    """
    Method to create MySql Connection.
    Enter host name, root, password and database name in connection statement.
    The function call another function createTables and redditDataExtraction.

    :return: none
    """

    mydb = mysql.connector.connect(host="HOST_NAME", user="DATABASE_USER",
                                   passwd="DATABASE_PASSWORD",
                                   database="DATABASE_NAME")
    myCursor = mydb.cursor()
    createTables(myCursor)
    redditDataExtraction(myCursor)
    mydb.commit()
    myCursor.close()
    mydb.close()


def createTables(myCursor):
    """
    This function creates table postData in the database. Character set is changed from
    utf8mb4 to utf8mb4_unicode_ci since post data can have emoji's.

    :param myCursor: SQL Cursor to execute sql query.
    :return: None
    """

    myCursor.execute("CREATE TABLE `postData` "
                     "(`submission_id` varchar(20) NOT NULL,`subreddit_name` varchar(200) "
                     "DEFAULT NULL,`submission_url` mediumtext,`submission_title` longtext,"
                     " `submission_body` longtext,`submission_date` datetime NOT NULL,"
                     "`No_of_comment` int(11) DEFAULT NULL,`score` int(11) DEFAULT "
                     "NULL,`author` varchar(50) NOT NULL,`polarity` double,`subjectivity`"
                     " double,`personal_pr` double,`possesive_pr` double,`adverb` double,"
                     "`verb` double,`noun` double,`readability` double ,`related`"
                     " char(3),`category` varchar(10) NOT NULL,`first_person` "
                     "int(11),`second_person` int(11), `third_person` int(11),`word_count`"
                     " int(11),`experientiality` float, `emotional_keyword` int(11) "
                     ",`social_life_keyword` int(11),`temporal_keyword` int(11),"
                     "`worklife_keyword` int(11), `cognitive_keyword` int(11),"
                     "`inhibition_keyword` int(11),`socialmedia_keyword` int(11) "
                     ",PRIMARY KEY(`submission_id`,`submission_date`)) "
                     "ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 "
                     "COLLATE=utf8mb4_unicode_ci;")


def redditObjectCreation():
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


def mhSubredditList():
    """
    List of mental health subreddits as defined in the mental health category of Reddit.

    :return: subreddit
    """

    subreddit = ['mentalhealth', 'traumatoolbox', 'bipolarreddit', 'BPD', 'ptsd',
                 'psychoticreddit', 'EatingDisorders', 'StopSelfHarm', 'survivorsofabuse',
                 'panicparty', 'socialanxiety', 'hardshipmates']
    return subreddit


def supportSubredditList():
    """
    List of support subreddits as defined in the support category of Reddit.

    :return: subreddit
    """

    subreddit = ['SuicideWatch', 'depression', 'Anxiety', 'foreveralone', 'socialanxiety']
    return subreddit


def nonMentalHealthSubredditList():
    """
    List of non mental health subreddits. These subreddits are the subreddits where
    social media keywords were more frequently occuring.

    :return: subreddit
    """

    subreddit = ['simpleliving', 'self', 'confession', 'confessions', 'AskReddit',
                 'TooAfraidToAsk', 'nosleep', 'nosurf', 'productivity', 'selfimprovement',
                 'Advice', 'antisocialmedia']
    return subreddit


def redditDataExtraction(myCursor):
    """
    Method calls redditObjectCreation() function which returns a reddit object.
    Functions mhSubredditList, supportSubredditList and nonMentalHealthSubredditList return
    list of subreddits from where post data is to be extracted. Datasource is checked while
    extracting posts since, sometimes news articles are also downloaded from reddit which has
    someother submission url than reddit.

    The data is extracted from reddit on the basis of simple keyword search enlisted in the
    variable search_keyword and is pushed to the database table postData through database cursor.

    :param myCursor: SQL Cursor to execute sql query.
    :return: None
    """

    redditObj = redditObjectCreation()
    mentalHealthSubreddit = mhSubredditList()
    supportGroupSubreddit = supportSubredditList()
    controlGroupSubreddit = nonMentalHealthSubredditList()

    submission_list = []
    datasource = "https://www.reddit.com"
    search_keyword = ['social media', 'instagram', 'facebook', 'twitter', 'snapchat']

    subredditList = [mentalHealthSubreddit, supportGroupSubreddit, controlGroupSubreddit]

    for category in subredditList:
        for subReddit in category:
            for keyword in search_keyword:
                for submissions in redditObj.subreddit(subReddit).search(keyword):
                    if datasource in submissions.url and submissions.id not in submission_list:

                        author = submissions.author.name
                        title = submissions.title
                        score = submissions.score
                        id = submissions.id
                        url = submissions.url
                        no_of_Comments = submissions.num_comments
                        created_dt = correct_date(submissions.created)
                        body = submissions.selftext
                        subreddit = submissions.subreddit.display_name
                        submission_list.append(id)
                        sql = "INSERT INTO postData(submission_id, subreddit_name, " \
                              "submission_url, submission_title, submission_body, " \
                              "submission_date, No_of_comment, " \
                              "score, author, category) values " \
                              "(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"

                        if category == mentalHealthSubreddit:
                            subred = "mh"

                        if category == supportGroupSubreddit:
                            subred = "support"

                        if category == controlGroupSubreddit:
                            subred = "nmh"

                        val = (id, subreddit, url, title, body.lower(), created_dt, no_of_Comments,
                               score, author, subred)
                        myCursor.execute(sql, val)
                    else:
                        pass


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
    dbConnection()
