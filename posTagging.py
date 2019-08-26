"""
@Author Kriti Upadhyaya

@Order of execution : 2nd file to be executed.

@ Dependencies :
pip install mysql
pip install textblob
pip install nltk


This program generates POS Tagging, calculate Sentiment Score, subjectivity score and
readability index and a first person, second person and third person pronoun count.
The values are then pushed to the postData table,  created in postExtraction.py,
against each submission id.
"""


import mysql.connector
from textblob import *
import nltk


def mysql_db():
    """
    The function creates a mysql connection to extract data from postData table.
    Enter host name, root, password and database name in connection statement.

    :return: None
    """

    myDB = mysql.connector.connect(host="HOST_NAME", user="DATABASE_USER",
                                   passwd="DATABASE_PASSWORD",
                                   database="DATABASE_NAME")
    myCursor = myDB.cursor()
    read_text(myCursor)
    myDB.commit()
    myDB.close()
    myCursor.close()


def read_text(myCursor):
    """
    The function executes a select command on table postData and pull submission id and
    submission body of all the submissions in postData. The function then calls function
    dataSet().

    :param myCursor: Database Cursor to execute queries.
    :return: None
    """

    myCursor.execute("select `submission_id`, `submission_body` from `postData`;")
    results = myCursor.fetchall()
    dataSet(results, myCursor)


def dataSet(results, myCursor):
    """
    The function reads the result set fetched from select command in read_text function.
    After extraction submission id and submission body one by one, the function makes call
    to other functions.

    :param results: a list of tuples. Each tuple has two values, at index 0 submission id
                    and at index 1 submission body.
    :param myCursor: SQL database cursor to execute queries.
    :return: None
    """

    for i in range(0, len(results)):
        submission_id = results[i][0]
        post = results[i][1]
        analyze_sentiment(post, submission_id, myCursor)
        posTagging(post, submission_id, myCursor)
        readabilityIndex(post, submission_id, myCursor)
        pronounCount(post, submission_id, myCursor)


def analyze_sentiment(text, id, myCursor):
    """
    The function first removes the stop words from submission body and then calculates
    sentiment score and subjectivity score using text blob. The corresponding fields
    of polarity i.e. sentiment score and subjectivity in the postData table are then updated
    using SQL Cursor against each submission id.

    :param text: submission body
    :param id: submission id
    :param myCursor: SQL cursor to execute query.
    :return: None
    """

    en_stop = set(nltk.corpus.stopwords.words('english'))
    stopWordFree = " ".join([j for j in text.split() if j not in en_stop])

    polarity = TextBlob(stopWordFree).sentiment.polarity
    subjectivity = TextBlob(stopWordFree).sentiment.subjectivity
    sql = "update postData SET polarity = %s, subjectivity = %s where submission_id = %s"
    val = (polarity, subjectivity, id)
    myCursor.execute(sql, val)


def posTagging(text, id, myCursor):
    """
    The function uses text blob for part of speech tagging. The POS tagged are:
    noun, personal pronoun, possessive pronoun, adverb and verb count. POS are
    tagged against each post and values are then pushed to postData table against
    corresponding submission id.

    :param text: submission body
    :param id: submission id
    :param myCursor: SQL Cursor to execute query.
    :return: None
    """

    verbCount = 0
    adverbCount = 0
    nounCount = 0
    personalPronounCount = 0
    possesivePronounCount = 0

    blob = TextBlob(text)
    for word, tag in blob.tags:
        if tag == 'NN' or tag == 'NNS':
            nounCount += 1
        if tag == 'PRP':
            personalPronounCount += 1
        if tag == 'PRP$':
            possesivePronounCount += 1
        if tag == 'RB' or tag == 'RBR' or tag == 'RBS':
            adverbCount += 1
        if tag == 'VB' or tag == 'VBD' or tag == 'VBG' or tag == 'VBN' or tag == 'VBN' \
                or tag == 'VBP' or tag == 'VBZ':
            verbCount += 1

    sql = "update postData set personal_pr = %s, possesive_pr = %s, adverb = %s, verb = %s, " \
          "noun = %s where submission_id = %s"
    val = (personalPronounCount, possesivePronounCount, adverbCount, verbCount, nounCount, id)
    myCursor.execute(sql, val)


def readabilityIndex(text, id, dbCursor):
    """
    Automated Readability Index is defined as a mechanism to compute the
    understandibility of text.
    ARI = 4.71(characters/ word) + 0.5(words/ sentences) - 21.43
    The function calculates ARI and update corresponding values in the postData table
    against corresponding submission id.

    :param text: submission body
    :param id: submission id
    :param dbCursor: SQL cursor to execute SQL query.
    :return: None
    """

    wordCount = 0
    charCount = 0
    sentCount = 0

    for word in text.split(" "):

        if len(word) < 15:
            wordCount += 1
            for c in word:
                charCount += 1

    for sentence in text.split("."):
        sentCount += 1

    if wordCount != 0:
        if sentCount == 0:
            sentCount = 1

        char_word = float(charCount / wordCount)

        sent_word = float(wordCount / sentCount)
        if char_word == 0:
            readability = 0
        else:

            readability = float((4.71 * char_word) + (0.5 * sent_word) - (21.43))

        sql = "update postData set readability = %s where submission_id = %s"
        val = (readability, id)
        dbCursor.execute(sql, val)


def pronounCount(text, id, dbCursor):
    """
    Function calculates number of first person, second person and third person
    pronouns used by user in its post. List of pronouns are collected from thesauras.
    The corresponding values are then updated in the postData table for easy access
    using SQL Cursor.

    :param text: submission body
    :param id: submission id
    :param dbCursor: SQL cursor to execute SQL query.
    :return: None
    """

    wordCount = 0
    firstPerson = 0
    secondPerson = 0
    thirdPerson = 0
    firstPP = ['i', 'me', 'mine', 'my', 'we', 'us', 'our', 'ours']
    secondPP = ['you', 'your', 'yours']
    thirdPP = ['he', 'she', 'it', 'him', 'her', 'his', 'its', 'hers',
               'they', 'them', 'their', 'theirs']

    for word in text.split():
        wordCount += 1
        if word in firstPP:
            firstPerson += 1
        elif word in secondPP:
            secondPerson += 1
        elif word in thirdPP:
            thirdPerson += 1
        else:
            pass

    sql = "update postData set first_person = %s, second_person = %s, third_person = %s," \
          " word_count = %s where submission_id = %s "
    val = (firstPerson, secondPerson, thirdPerson, wordCount, id)
    dbCursor.execute(sql, val)


if __name__ == "__main__":
    mysql_db()
