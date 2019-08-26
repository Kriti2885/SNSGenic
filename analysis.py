"""
@Author Kriti Upadhyaya

@Order of execution : 6

@ Dependencies :
pip install mysql


This program is used to analyse the common patterns prevailing in the submissions
in different categories like the average post length, average number of first
person pronouns, average number of second person and third person pronouns etc.
"""

import mysql.connector


def mysqlDB():
    """
    The function creates a mysql connection to extract data from postData table.
    Enter host name, root, password and database name in connection statement.

    :return: None
    """

    dbConnection = mysql.connector.connect(host="HOST_NAME", user="DATABASE_USER",
                                   passwd="DATABASE_PASSWORD",
                                   database="DATABASE_NAME")
    dbCursor = dbConnection.cursor()
    getResult(dbCursor)
    dbCursor.close()
    dbConnection.close()


def getResult(dbCursor):
    """
    The function executes a select command on table postData and pull corresponding average
    values from postData table. The function then calls function extractData.
    dataSet().

    """

    # For mental health category subreddits

    dbCursor.execute("select avg(first_person), avg(second_person), avg(third_person),"
                     "avg(word_count), avg(emotional_keyword), avg(social_life_keyword),"
                     "avg(temporal_keyword), avg(worklife_keyword), avg(cognitive_keyword),"
                     "avg(inhibition_keyword), avg(socialmedia_keyword), avg(deletion), avg(polarity)"
                     " from `postData` where `category`='mh';")
    mentalHealth = dbCursor.fetchone()

    # For non mental health subreddits

    dbCursor.execute("select avg(first_person), avg(second_person), avg(third_person),"
                     "avg(word_count), avg(emotional_keyword), avg(social_life_keyword),"
                     "avg(temporal_keyword), avg(worklife_keyword), avg(cognitive_keyword),"
                     "avg(inhibition_keyword), avg(socialmedia_keyword), avg(deletion), avg(polarity)"
                     " from `postData` where `category`='nmh';")
    nmHealth = dbCursor.fetchone()

    # For support subreddit category

    dbCursor.execute("select avg(first_person), avg(second_person), avg(third_person),"
                     "avg(word_count), avg(emotional_keyword), avg(social_life_keyword),"
                     "avg(temporal_keyword), avg(worklife_keyword), avg(cognitive_keyword),"
                     "avg(inhibition_keyword), avg(socialmedia_keyword), avg(deletion), avg(polarity)"
                     " from `postData` where `category`='support';")
    support = dbCursor.fetchone()
    extractData(mentalHealth, nmHealth, support)


def extractData(mentalHealth, nmHealth, support):
    """

    :param mentalHealth: average values of mental health subreddits
    :param nmHealth: average values of non mental health subreddits
    :param support: average values of support subreddits.
    :return: None
    """

    # Data presentation for average first person pronoun count in all three categories of subreddits.
    avgFirstPPM = mentalHealth[0]
    avgFirstPPN = nmHealth[0]
    avgFirstPPS = support[0]

    print("The average First Person Pronoun in mental Health, non mental health, support Subreddit is "
          + str(avgFirstPPM) + ", " +str(avgFirstPPN) + ", " + str(avgFirstPPS))

    # Data presentation for average second person pronoun count in all three categories of subreddits.
    avgSecondPPM = mentalHealth[1]
    avgSecondPPN = nmHealth[1]
    avgSecondPPS = support[1]

    print("The average Second Person Pronoun in mental Health, non mental health, support Subreddit is "
          + str(avgSecondPPM) + ", " + str(avgSecondPPN) + ", " + str(avgSecondPPS))

    # Data presentation for average third person pronoun count in all three categories of subreddits.
    avgThirdPPM = mentalHealth[2]
    avgThirdPPN = nmHealth[2]
    avgThirdPPS = support[2]

    print("The average Third Person Pronoun in mental Health, non mental health, support Subreddit is "
          + str(avgThirdPPM) + ", " + str(avgThirdPPN) + ", " + str(avgThirdPPS))

    # Data presentation for average word count in all three categories of subreddits.
    avgWordCountM = mentalHealth[3]
    avgWordCountN = nmHealth[3]
    avgWordCountS = support[3]

    print("The average word count in mental Health, non mental health, support Subreddit is "
          + str(avgWordCountM) + ", " + str(avgWordCountN) + ", " + str(avgWordCountS))

    # Data presentation for average emotional keyword count in all three categories of subreddits.
    avgEmoM = mentalHealth[4]
    avgEmoN = nmHealth[4]
    avgEmoS = support[4]

    print("The average emotional keyword count in mental Health, non mental health, support Subreddit is "
          + str(avgEmoM) + ", " + str(avgEmoN) + ", " + str(avgEmoS))

    # Data presentation for average social life keyword count in all three categories of subreddits.
    avgSocialM = mentalHealth[5]
    avgSocialN = nmHealth[5]
    avgSocialS = support[5]

    print("The average social keyword count in mental Health, non mental health, support Subreddit is "
          + str(avgSocialM) + ", " + str(avgSocialN) + ", " + str(avgSocialS))

    # Data presentation for average temporal keyword count in all three categories of subreddits.
    avgTempM = mentalHealth[6]
    avgTempN = nmHealth[6]
    avgTempS = support[6]

    print("The average temporal keyword count in mental Health, non mental health, support Subreddit is "
          + str(avgTempM) + ", " + str(avgTempN) + ", " + str(avgTempS))

    # Data presentation for average work-life keyword count in all three categories of subreddits.
    avgWorkM = mentalHealth[7]
    avgWorkN = nmHealth[7]
    avgWorkS = support[7]

    print("The average work related keyword count in mental Health, non mental health, support Subreddit is "
          + str(avgWorkM) + ", " + str(avgWorkN) + ", " + str(avgWorkS))

    # Data presentation for average cognition keyword count in all three categories of subreddits.
    avgCogniM = mentalHealth[8]
    avgCogniN = nmHealth[8]
    avgCogniS = support[8]

    print("The average cognition keyword count in mental Health, non mental health, support Subreddit is " +
         str(avgCogniM) + ", " + str(avgCogniN) + ", " + str(avgCogniS))

    # Data presentation for average inhibition keyword count in all three categories of subreddits.
    avginhiM = mentalHealth[9]
    avginhiN = nmHealth[9]
    avginhiS = support[9]

    print("The average inhibition keyword count in mental Health, non mental health, support Subreddit is " +
          str(avginhiM) + ", " + str(avginhiN) + ", " + str(avginhiS))

    # Data presentation for average social media keyword count in all three categories of subreddits.
    avgSocialM = mentalHealth[10]
    avgSocialN = nmHealth[10]
    avgSocialS = support[10]

    print("The average social media keyword count in mental Health, non mental health, support Subreddit is "
          + str(avgSocialM) + ", " + str(avgSocialN) + ", " + str(avgSocialS))

    # Data presentation for average deletion keyword count in all three categories of subreddits.
    avgDelM = mentalHealth[11]
    avgDelN = nmHealth[11]
    avgDelS = support[11]

    print("The average deletion keyword count in mental Health, non mental health, support Subreddit is "
          + str(avgDelM) + ", " + str(avgDelN) + ", " + str(avgDelS))

    # Data presentation for average sentiment score in all three categories of subreddits.
    avgPolM = mentalHealth[12]
    avgPolN = nmHealth[12]
    avgPolS = support[12]

    print("The average sentiment score in mental Health, non mental health, support Subreddit is "
          + str(avgPolM) + ", " + str(avgPolN) + ", " + str(avgPolS))


if __name__ == "__main__":
    mysqlDB()
