'''
@Author Kriti Upadhyaya

@Order of Execution : 5

@Dependencies:

pip install mysql
pip install nltk
pip install emoji
pip install textblob
pip install gensim

This program implements LDA Algorithm for topic modelling.
This program uses two approaches for dictionary build up in LDA : Bag of words and Part of Speech Tag.
'''

import nltk
import re
import emoji
import string
import mysql.connector
from pprint import pprint
from textblob import TextBlob
from gensim import corpora, models
from gensim.models import CoherenceModel
from nltk.stem.wordnet import WordNetLemmatizer

documentList = list()
bagOfWordDictionary = list()


def mysqlDB():
    """
    The function creates a mysql connection to extract data from postData table.
    Enter host name, root, password and database name in connection statement.
    The function calls function createTable to create tables for unigram , bigram
    and trigrams in the database. Then it calls function extractPost.

    :return: None
    """

    dbConnection = mysql.connector.connect(host="HOST_NAME", user="DATABASE_USER",
                                            passwd="DATABASE_PASSWORD",
                                            database="DATABASE_NAME")
    dbCursor = dbConnection.cursor()
    readText(dbCursor)
    dbCursor.close()
    dbConnection.close()


def readText(dbCursor):
    """
    Select query pulls submission id and submission body from postData table. The
    result set returned is a list of tuples with submission id and submission body.
    The function calls createDataSet function to extract submission id and submission
    body from resultset and preprocess the data.

    :param dbCursor: Mysql Cursor to execute queries.
    :return: None
    """

    dbCursor.execute("select distinct `submission_id`, `submission_body` from `postData` where deletion > 0;")
    resultSet = dbCursor.fetchall()
    createDataSet(resultSet)
    dictionaryBuildUp()


def createDataSet(dataSet):
    """
    The function reads the result set fetched from select command in readText function.
    After extracting submission id and submission body, the function makes call
    to dataCleaning function which pre-process the text data. The function uses a global
    dictionary to store the cleaned text in it.

    :param dataSet: a list of tuples. Each tuple has three values, at index 0 submission id
                    and at index 1 submission body at index 2 category of submission i.e.
                    mental health or non mental health or support.

    :return: None
    """
    global documentList
    for i in range(0, len(dataSet)):
        submissionID = dataSet[i][0]
        postData = dataSet[i][1]
        cleanedData = dataCleaning(postData)
        documentList.append(cleanedData)


def dataCleaning(postData):
    """
    The purpose of this function is to return a processed stream of text after
    removing stop words, emojis, punctuation, web links and then lemmatizing.
    Different functions have been called for each of these text processing tasks.

    :param postData: Submission body
    :return: A stream of processed text for dictionary creation of LDA Algorithm.
    """
    global bagOfWordDictionary
    linkFree = stopLinks(postData)
    stopFree = stopWordRemoval(linkFree)
    emojiFree = removeEmoji(stopFree)
    punctuationFree = punctuationRemoval(emojiFree)
    lemmatiedWordStream = lemmatizeStream(punctuationFree)
    nounVerbStream = nounExtraction(lemmatiedWordStream)
    bagOfWord = bowApproach(lemmatiedWordStream)
    bagOfWordDictionary.append(bagOfWord)
    tokenPostData = tokenStream(nounVerbStream)
    return tokenPostData


def stopLinks(postData):
    """
    The function removes web links from submission body and return a processed
    stream of text.

    :param postData: Submission body
    :return: Text without any web links.
    """

    linkFreeData = " ".join([word for word in postData.split() if len(word) < 15])
    return linkFreeData


def stopWordRemoval(postData):
    """
    The function removes stopwords from the submission body using nltk stop words
    and return a stop word free submission body.

    :param postData: Submission body after web link removal.
    :return: stop word cleaned submission body.
    """

    en_stop = set(nltk.corpus.stopwords.words('english'))
    stopFree = " ".join([j for j in postData.lower().split() if j not in en_stop])
    regex = re.compile('[^a-zA-Z]')
    stopFreeText = regex.sub(' ', stopFree)
    lenRestrictedText = " ".join([word for word in stopFreeText.split() if len(word)>3])
    return lenRestrictedText


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


def bowApproach(postData):
    """
    The function is used for extracting tokenised stream of submissions for Bag of Words
    implementation of LDA Algorithm.

    :param postData: Submission data after data cleaning.
    :return: tokenised stream of cleaned data.
    """
    bagOfWord = tokenStream(postData)
    return bagOfWord


def nounExtraction(postData):
    """
    The function is used to extract nouns from the submissions and creating a
    stream of nouns only per submission to implement POS approah in LDA.

    :param postData: Cleaned Data from text pre-processing functions.
    :return: nounVerbDocument : noun stream of submission data.
    """

    nounVerbDocument = ""
    blob = TextBlob(postData)
    for word, tag in blob.tags:
        if tag == 'NN' or tag == 'NNS':
            nounVerbDocument += word + " "
    return nounVerbDocument


def tokenStream(postData):
    """
    The function takes input a word stream and tokenise it.

    :param postData: word stream which is to be tokenised.
    :return: tokenised stream
    """
    tokenisedStream = postData.split()
    return tokenisedStream


def dictionaryBuildUp():
    """
    The function converts text into dictionary. Each submission is converted to a document
    term matrikx which is then given input into the LDA Algorithm model. This function
    converts the tokenised stream as a bag of word and then converts it into a m*n document
    term matrix.
    The function calls modelling function.

    :return:
    """
    global documentList, bagOfWordDictionary
    dictionaryNN = corpora.Dictionary(documentList)
    documentTermMatrix = [dictionaryNN.doc2bow(doc) for doc in documentList]
    modelType = "POS"
    modelling(documentTermMatrix, dictionaryNN, modelType)

    dictionaryBOW = corpora.Dictionary(bagOfWordDictionary)
    documentTermMatrixBOW = [dictionaryBOW.doc2bow(text) for text in bagOfWordDictionary]
    modelType = "BOW"
    modelling(documentTermMatrixBOW, dictionaryBOW, modelType)


def modelling(matrix, dictionary, modelType):
    """
    The function implements the LDA Algorithm for topic modelling using two approaches-
    bag of words and part of speech tag. The function extract the topic from corpus.
    The function also evaluates the performance of LDA in terms of perplexity and coherence.

    :param matrix: Document term matrix which is generated in the dictionaryBuildUp function.
    :param dictionary: dictionary of submissions
    :param modelType: Implementation of LDA is in two approaches Bag of word and part
    of speech tag. This parameter tells which one is implemented.
    :return:None
    """
    global documentList, bagOfWordDictionary

    if modelType == "POS":

        lda_model = models.LdaModel(corpus=matrix, num_topics=6, id2word=dictionary, passes=30)
        print("LDA Model:")
        pprint(lda_model.print_topics(num_topics=6, num_words=5))
        print('Perplexity with POS: ', lda_model.log_perplexity(matrix))
        coherenceModelLDA = CoherenceModel(model=lda_model, texts=documentList,
                                           dictionary=dictionary, coherence='c_v')
        coherenceLDA = coherenceModelLDA.get_coherence()
        print('Coherence Score with POS: ', coherenceLDA)

    elif modelType == "BOW":

        lda_model = models.LdaModel(corpus=matrix, num_topics=6, id2word=dictionary, passes=30)
        print("LDA Model:")
        pprint(lda_model.print_topics(num_topics=6, num_words=5))
        print('Perplexity with BOW: ', lda_model.log_perplexity(matrix))
        coherenceModelLDA = CoherenceModel(model=lda_model, texts=bagOfWordDictionary,
                                           dictionary=dictionary, coherence='c_v')
        coherenceLDA = coherenceModelLDA.get_coherence()
        print('Coherence Score with BOW: ', coherenceLDA)

    else:
        pass


if __name__ == "__main__":
    mysqlDB()
