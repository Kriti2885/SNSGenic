"""
@Author: Kriti Upadhyaya

@Order of Execution : 4

@Dependencies:
pip install mysql

Using the unigrams, bigrams and trigrams generated in nGram.py,
I hand selected few unigrams and categorised them into 7 categories.
We used these keyword counts as an input to the random forest algorithm.
"""


import mysql.connector


def mysqlDB():
    """
    The function creates a mysql connection to extract data from postData table.
    Enter host name, root, password and database name in connection statement.
    The function calls function readText to extract data from database.

    :return: None
    """

    dbConnection = mysql.connector.connect(host="HOST_NAME", user="DATABASE_USER",
                                   passwd="DATABASE_PASSWORD",
                                   database="DATABASE_NAME")
    dbCursor = dbConnection.cursor()
    readText(dbCursor)
    dbConnection.commit()
    dbCursor.close()
    dbConnection.close()


def readText(dbCursor):
    """
    Select query pulls submission id and submission body from postData table. The
    result set returned is a list of tuples with submission id and submission body.
    The function calls createDataSet function to extract submission id and submission
    body from resultset.

    :param myCursor: Mysql Cursor to execute queries.
    :return: None
    """

    dbCursor.execute("select distinct `submission_id`, `submission_body` from `postData`;")
    resultSet = dbCursor.fetchall()
    createDataSet(resultSet, dbCursor)


def createDataSet(postData, myCursor):
    """
    The function reads the result set fetched from select command in readText function.
    After extracting submission id and submission body one by one, the function makes call
    to keyWordCount function.

    :param results: a list of tuples. Each tuple has three values, at index 0 submission id
                    and at index 1 submission body at index 2 category of submission i.e.
                    mental health or non mental health or support.
    :param myCursor: SQL database cursor to execute queries.
    :return: None
    """

    for i in range(0, len(postData)):
        submissionID= postData[i][0]
        submissionBody = postData[i][1]
        keywordCount(submissionBody, submissionID, myCursor)


def keywordCount(submissionData, submissionID, myCursor):
    """
    The function counts number of occurences of hand curated keywords categorised under 7
    categories. This list has been made from the unigrams, bigrams and trigrams lists. The
    idea behind is to take the number of keyword count as a feature to the Random Forest Algorithm.
    The function, after counting, makes relevant entries in the database against each submission id.

    :param submissionData: Submission body
    :param submissionID: submisison id
    :param myCursor: SQL cursor to execute query
    :return:
    """
    expCount = 0
    relCount = 0
    tempoCount = 0
    workCount = 0
    cogniCount = 0
    inhibCount = 0
    socialCount = 0
    deletion = 0

    expressions = ['happy', 'sad', 'love', 'anxiety', 'good', 'hate', 'depressed', 'bad',
                   'anxious', 'lonely']
    relationships = ['family', 'friends', 'people', 'person', 'parents', 'wife', 'partner',
                     'husband', 'relationship', 'girlfriend', 'boyfriend', 'son', 'daughter',
                     'ex', 'gf', 'bf', 'crush']
    temporal = ['time', 'day', 'years', 'months', 'hours', 'future', 'daily']
    worklife = ['life', 'school', 'work', 'job']
    cognition = ['hard', 'lot', 'like', 'feel', 'know', 'think', 'see', 'felt', 'wanted',
                 'thought', 'saw']
    inhibition = ['avoid', 'deny', 'safe', 'never', 'quit', 'remove']
    socialMedia = ['facebook', 'social media', 'snapchat', 'twitter', 'instagram',
                   'social network', 'follow', 'retweet', 'tweet', 'insta', 'follower',
                   'hashtag', 'tag', 'messenger', 'group']
    deletionList = ['delete', 'deleted', 'deactivate', 'deactivated']

    for word in submissionData.split(" "):
        if word in expressions:
            expCount += 1
        if word in relationships:
            relCount += 1
        if word in temporal:
            tempoCount += 1
        if word in worklife:
            workCount += 1
        if word in cognition:
            cogniCount += 1
        if word in inhibition:
            inhibCount += 1
        if word in socialMedia:
            socialCount += 1
        if word in deletionList:
            deletion += 1
        else:
            pass

    sql = "update postData set emotional_keyword = %s, social_life_keyword = %s, temporal_keyword = %s, " \
          "worklife_keyword = %s, cognitive_keyword = %s, inhibition_keyword = %s, socialmedia_keyword = %s, " \
          "deletion = %s where submission_id = %s"
    val = (expCount, relCount, tempoCount, workCount, cogniCount, inhibCount, socialCount, deletion, submissionID)
    myCursor.execute(sql, val)


if __name__ == "__main__":
    mysqlDB()
