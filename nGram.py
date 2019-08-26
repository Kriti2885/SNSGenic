"""
@Author Kriti Upadhyaya

@Order of Execution : 3

@Dependencies:
pip install mysql
pip install nltk
pip install emoji

This program generates unigram, bigram and trigrams and record their frequencies
in all the posts. The nGrams are then pushed to corresponding tables in the
database which are created through this program. The ngrams are generated on
pre-processed submission text. The pre-processing is done by removing stop words,
emojis, punctuation, web links and then lemmatizing.
"""

import re
import nltk
import emoji
import string
import mysql.connector
from collections import defaultdict
from nltk.stem.wordnet import WordNetLemmatizer


unigramDict = defaultdict(dict)
bigramDict = defaultdict(dict)
trigramDict = defaultdict(dict)


def mysql_db():
    """
    The function creates a mysql connection to extract data from postData table.
    Enter host name, root, password and database name in connection statement.
    The function calls function createTable to create tables for unigram , bigram
    and trigrams in the database. Then it calls function extractPost.

    :return: None
    """
    myDB = mysql.connector.connect(host="HOST_NAME", user="DATABASE_USER",
                                   passwd="DATABASE_PASSWORD",
                                   database="DATABASE_NAME")
    myCursor = myDB.cursor()
    createTable(myCursor)
    extractPost(myCursor)
    myDB.commit()
    myCursor.close()
    myDB.close()


def createTable(myCursor):
    """
    The function creates three table in database for unigram, bigram and trigram
    with fields mentalHealth, nonMentalHealth and support. These fields will have
    individual frequencies of unigrams, bigram and trigram in posts related to mental
    health, non mental health and support subreddit.

    :param myCursor: Mysql Cursor to execute queries.
    :return: None
    """

    myCursor.execute(
        "CREATE TABLE `unigram`(`unigram` varchar(50) NOT NULL, `mentalHealth`"
        " integer , `nonMentalHealth` integer, `support` integer, `frequency`"
        " int(11) DEFAULT NULL, `idf` double DEFAULT NULL, PRIMARY KEY(`unigram`) ) "
        "ENGINE = InnoDB DEFAULT CHARSET = utf8;")

    myCursor.execute(
        "CREATE TABLE `bigram`(`bigram` varchar(50) NOT NULL, `mentalHealth` integer"
        " , `nonMentalHealth` integer, `support` integer, `frequency` int(11) "
        "DEFAULT NULL, `idf` double DEFAULT NULL, PRIMARY KEY(`bigram`) ) "
        "ENGINE = InnoDB DEFAULT CHARSET = utf8;")

    myCursor.execute(
        "CREATE TABLE `trigram`(`trigram` varchar(50) NOT NULL, `mentalHealth` "
        "integer , `nonMentalHealth` integer, `support` integer, `frequency` "
        "int(11) DEFAULT NULL, `idf` double DEFAULT NULL, PRIMARY KEY(`trigram`) )"
        " ENGINE = InnoDB DEFAULT CHARSET = utf8;")


def extractPost(myCursor):
    """
    Select query pulls submission id and submission body from postData table. The
    result set returned is a list of tuples with submission id and submission body.
    The function calls readData set function to extract submission id and submission
    body from resultset.

    :param myCursor: Mysql Cursor to execute queries.
    :return: None
    """
    myCursor.execute("select `submission_id`, `submission_body`, `category` from "
                     "`postData`")
    results = myCursor.fetchall()
    readDataSet(results, myCursor)


def readDataSet(results, myCursor):
    """
    The function reads the result set fetched from select command in extractPost function.
    After extraction submission id and submission body one by one, the function makes call
    to other functions.

    :param results: a list of tuples. Each tuple has three values, at index 0 submission id
                    and at index 1 submission body at index 2 category of submission i.e.
                    mental health or non mental health or support.
    :param myCursor: SQL database cursor to execute queries.
    :return: None
    """

    for i in range(0, len(results)):

        submissionID = results[i][0]
        submissionBody = results[i][1]
        category = results[i][2]
        cleanedText = textPreProcess(submissionBody)
        generateUnigram(cleanedText, category)
        generateBigram(cleanedText, category)
        generateTrigram(cleanedText, category)

    makeEntryUnigram(myCursor)
    makeEntryBigram(myCursor)
    makeEntryTrigram(myCursor)


def textPreProcess(postData):
    """
    The purpose of this function is to return a processed stream of text after
    removing stop words, emojis, punctuation, web links and then lemmatizing.
    Different functions have been called for each of these text processing tasks.

    :param postData: Submission body
    :return: A stream of processed text for generation of unigram,
            bigram and trigram.
    """

    linkFree = stopLinks(postData)
    emojiFree = removeEmoji(linkFree)
    punctuationFree = punctuationRemoval(emojiFree)
    stopFree = stopWordRemoval(punctuationFree)
    lemmatiedWordStream = lemmatizeStream(stopFree)
    return lemmatiedWordStream


def stopLinks(postData):
    """
    The function removes web links from submission body and return a processed
    stream of text.

    :param postData: Submission body
    :return: Text without any web links.
    """
    linkFreeData = " ".join([word for word in postData.split() if (2 < len(word) < 15)])
    return linkFreeData


def stopWordRemoval(postData):
    """
    The function removes stopwords from the submission body using nltk stop words
    and return a stop word free submission body.

    :param postData: Submission body after web link removal.
    :return: stop word cleaned submission body.
    """

    en_stop = set(nltk.corpus.stopwords.words('english'))
    stopFree = " ".join([j for j in postData.split() if j not in en_stop])
    regex = re.compile('[^a-zA-Z]')
    stopFreeText = regex.sub(' ', stopFree)
    return stopFreeText


def removeEmoji(postData):
    """
    The function removes emoji from the submission body using python inbuilt emoji function

    :param postData: Submission body after web link removal and stop word removal.
    :return: emoji free submission text.
    """

    allchars = [str for str in postData]
    emoji_list = [c for c in allchars if c in emoji.UNICODE_EMOJI]
    clean_text = ' '.join([str for str in postData.split() if not any(i in str for i in emoji_list)])
    return clean_text


def punctuationRemoval(postData):
    """
    The function removes punctuations from the cleaned text.

    :param postData: Submission body after web link, stop word and emoji removal.
    :return: Punctuation free submission body.
    """

    punctuation = set(string.punctuation)
    puncFree = ''.join(ch for ch in postData if ch not in punctuation)
    return puncFree


def lemmatizeStream(postData):
    """
    The function lemmatise the text stream set as input and return the same.

    :param postData: Submission body after web link, stop word, punctuation and emoji removal.
    :return: lemmatized submission body
    """

    lemmatizer = WordNetLemmatizer()
    normalizedPostData = " ".join(lemmatizer.lemmatize(word) for word in postData.split())
    return normalizedPostData


def generateUnigram(text, category):
    """
    The function generates unigrams. Unigrams are single words. Frequency of all the
    unigrams are also recorded and then saved in dictionary.

    :param text: preprocessed submission body
    :param category: subreddit category of submission
    :return: None
    """

    global unigramDict
    for word in text.split():
        if any(char.isdigit() for char in word) and not any(char.isalpha for char in word):
            pass
        else:
            if category == "mh":
                if word not in unigramDict:
                    unigramDict[word] = {}
                    unigramDict[word]['mh'] = 1

                else:
                    if "mh" in unigramDict[word]:
                        unigramDict[word]['mh'] += 1
                    else:
                        unigramDict[word]['mh'] = 1

            elif category == "nmh":
                if word not in unigramDict:
                    unigramDict[word] = {}
                    unigramDict[word]['nmh'] = 1

                else:
                    if "nmh" in unigramDict[word]:
                        unigramDict[word]['nmh'] += 1
                    else:
                        unigramDict[word]['nmh'] = 1

            elif category == "support":
                if word not in unigramDict:
                    unigramDict[word] = {}
                    unigramDict[word]['support'] = 1

                else:
                    if "support" in unigramDict[word]:
                        unigramDict[word]['support'] += 1
                    else:
                        unigramDict[word]['support'] = 1


def makeEntryUnigram(dbCursor):
    """
    The function reads unigrams and their frequencies from the dictionary and then
    make entry in the database against each category.

    :param dbCursor: Sql Cursor to execute query
    :return: None
    """

    global unigramDict

    for unigram in unigramDict:

        freqInMH = unigramDict[unigram]['mh']
        freqInNMH = unigramDict[unigram]['nmh']
        freqINSupport = unigramDict[unigram]['support']

        sql = "Insert into `unigram`(unigram, mentalHealth, nonMentalHealth, support) " \
              "values(%s, %s, %s, %s)"
        val = (unigram, freqInMH, freqInNMH, freqINSupport)
        dbCursor.execute(sql, val)


def generateBigram(post, category):
    """
    The function generates bigrams. bigrams are combination of two consequent words.
    Frequency of all the bigrams are also recorded and stored in dictionary.

    :param text: preprocessed submission body
    :param category: subreddit category of submission
    :return: None
    """

    global bigramDict

    text = post.split()
    for i in range(0, len(text)-1):
        bigram = text[i] + " " + text[i+1]

        if category == "mh":
            if bigram not in bigramDict:
                bigramDict[bigram] = {}
                bigramDict[bigram]['mh'] = 1

            else:
                if "mh" in bigramDict[bigram]:
                    bigramDict[bigram]['mh'] += 1
                else:
                    bigramDict[bigram]['mh'] = 1

        elif category == "nmh":
            if bigram not in bigramDict:
                bigramDict[bigram] = {}
                bigramDict[bigram]['nmh'] = 1

            else:
                if "nmh" in bigramDict[bigram]:
                    bigramDict[bigram]['nmh'] += 1
                else:
                    bigramDict[bigram]['nmh'] = 1

        elif category == "support":
            if bigram not in bigramDict:
                bigramDict[bigram] = {}
                bigramDict[bigram]['support'] = 1

            else:
                if "support" in bigramDict[bigram]:
                    bigramDict[bigram]['support'] += 1
                else:
                    bigramDict[bigram]['support'] = 1


def makeEntryBigram(myCursor):
    """
    The function reads bigram and their frequencies from the dictionary and then
    make entry in the database against each category.

    :param dbCursor: Sql Cursor to execute query
    :return: None
    """
    global bigramDict

    for bigram in bigramDict:

        freqInMH = bigramDict[bigram]['mh']
        freqInNMH = bigramDict[bigram]['nmh']
        freqINSupport = bigramDict[bigram]['support']

        sql = "Insert into `bigram`(bigram, mentalHealth, nonMentalHealth, support) " \
              "values(%s, %s, %s, %s)"
        val = (bigram, freqInMH, freqInNMH, freqINSupport)
        myCursor.execute(sql, val)


def generateTrigram(post, category):
    """
    The function generates trigrams. Trigrams are combination of three consequent words.
    Frequency of all the trigrams are also recorded and stored in dictionary.

    :param text: preprocessed submission body
    :param category: subreddit category of submission
    :return: None
    """

    text = post.split()

    global trigramDict
    for i in range(0, len(text)-2):

        trigram = text[i] + " " + text[i+1] + " " + text[i+2]
        if category == "mh":
            if trigram not in trigramDict:
                trigramDict[trigram] = {}
                trigramDict[trigram]['mh'] = 1

            else:
                if "mh" in trigramDict[trigram]:
                    trigramDict[trigram]['mh'] += 1
                else:
                    trigramDict[trigram]['mh'] = 1

        elif category == "nmh":
            if trigram not in trigramDict:
                trigramDict[trigram] = {}
                trigramDict[trigram]['nmh'] = 1

            else:
                if "nmh" in trigramDict[trigram]:
                    trigramDict[trigram]['nmh'] += 1
                else:
                    trigramDict[trigram]['nmh'] = 1

        elif category == "support":
            if trigram not in trigramDict:
                trigramDict[trigram] = {}
                trigramDict[trigram]['support'] = 1

            else:
                if "support" in trigramDict[trigram]:
                    trigramDict[trigram]['support'] += 1
                else:
                    trigramDict[trigram]['support'] = 1


def makeEntryTrigram(myCursor):
    """
    The function reads trigram from global dictionary and their frequencies and then
    make entry in the database against each category.

    :param dbCursor: Sql Cursor to execute query
    :return: None
    """
    global trigramDict
    for trigram in trigramDict:

        freqInMH = trigramDict[trigram]['mh']
        freqInNMH = trigramDict[trigram]['nmh']
        freqINSupport = trigramDict[trigram]['support']

        sql = "Insert into `trigram`(trigram, mentalHealth, nonMentalHealth, support) " \
              "values(%s, %s, %s, %s)"
        val = (trigram, freqInMH, freqInNMH, freqINSupport)
        myCursor.execute(sql, val)


if __name__ == "__main__":
    mysql_db()

