import mysql.connector
from textblob import *
import nltk
import json
import math
import string
import re
import emoji
from nltk.stem.wordnet import WordNetLemmatizer
from nltk.stem.porter import PorterStemmer
from collections import defaultdict

unigramDictionary = defaultdict(dict)
wordFrequency = defaultdict()


def mysqlDB():

    dbConnection = mysql.connector.connect(host="localhost", user="root", passwd="kritidb", database="new_praw_db")
    dbCursor = dbConnection.cursor()
    readText(dbCursor, dbConnection)
    dbCursor.close()
    dbConnection.close()


def readText(dbCursor, dbConnection):

    dbCursor.execute("select distinct `submission_id`, `submission_body` from `hand_annotate` where "
                     "`category` = 'support';")
    resultSet = dbCursor.fetchall()
    totalSubmission = len(resultSet)
    createDataSet(resultSet)
    idfScoring(totalSubmission, dbCursor, dbConnection)


def createDataSet(resultSet):
    for i in range(0, len(resultSet)):
        submissionID =resultSet[i][0]
        postData = resultSet[i][1]
        dataSet = dataCleaning(postData)
        unigramGenerator(dataSet, submissionID)


def dataCleaning(postData):

    stopFree = stopWordRemoval(postData)
    emojiFree = removeEmoji(stopFree)
    punctuationFree = punctuationRemoval(emojiFree)
    lemmatiedWordStream = lemmatizeStream(punctuationFree)
    stemmedPostStream = dataStemmer(lemmatiedWordStream)
    return stemmedPostStream


def unigramGenerator(postData, submissionID):

    global unigramDictionary
    global wordFrequency
    for word in postData:
        if any(char.isdigit() for char in word) and not any(char.isalpha for char in word):
            pass
        else:
            if word not in unigramDictionary:
                unigramDictionary[word] = {}
                unigramDictionary[word][submissionID] = 1
            elif word in unigramDictionary:
                child_dict = unigramDictionary[word]
                if submissionID in child_dict:
                    freq = child_dict[submissionID]
                    unigramDictionary[word][submissionID] = freq + 1
                else:
                    unigramDictionary[word][submissionID] = 1
            else:
                pass

            if word in wordFrequency:
                wordFrequency[word] += 1
            else:
                wordFrequency[word] = 1
    print unigramDictionary


def stopWordRemoval(postData):

    en_stop = set(nltk.corpus.stopwords.words('english'))
    stopFree = " ".join([j for j in postData.lower().split() if j not in en_stop])
    regex = re.compile('[^a-zA-Z]')
    stopFreeText = regex.sub(' ', stopFree)
    return stopFreeText


def removeEmoji(postData):

    allchars = [str for str in postData]
    emoji_list = [c for c in allchars if c in emoji.UNICODE_EMOJI]
    clean_text = ' '.join([str for str in postData.split() if not any(i in str for i in emoji_list)])
    return clean_text


def punctuationRemoval(postData):

    punctuation = set(string.punctuation)
    puncFree = ''.join(ch for ch in postData if ch not in punctuation)
    return puncFree


def lemmatizeStream(postData):

    lemmatizer = WordNetLemmatizer()
    normalizedPostData = " ".join(lemmatizer.lemmatize(word) for word in postData.split())
    return normalizedPostData


def dataStemmer(postData):

    stemmer = PorterStemmer()
    stemmingData = stemmer.stem(postData)
    stemmedData = stemmingData.split()
    return stemmedData


def idfScoring(totalSubmission, dbCursor, dbConnection):

    global wordFrequency
    global unigramDictionary
    for unigram in wordFrequency:

        freqOfWord = wordFrequency[unigram]
        child_dict = unigramDictionary[unigram]
        wordInDoc = len(child_dict.keys())
        idf = math.log((totalSubmission / wordInDoc), 10)
        sql = "Insert into `hand_annotate_unigram_support`(word, total_freq, idf) values(%s, %s, %s)"
        val = (unigram, freqOfWord, idf)
        dbCursor.execute(sql, val)
        dbConnection.commit()


if __name__ == "__main__":
    mysqlDB()
