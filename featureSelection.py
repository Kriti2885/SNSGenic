"""
@Author: John Gann

@ Modified By : Kriti Upadhyaya

@Modification made:
Added Part of Speech Tag, Changes in keyword categories and adding more keywords.

@ Dependencies:
pip install textstat
pip install sklearn

The code was modified while performance tuning of Random Forest Classifier by adding or
subtracting feature sets. The programme selects the features and make corresponding
entries against each submission that are give to the random forest algorithm.
"""

import cPickle as pickle
import textblob
import re
import os
import textstat
from nltk.stem import PorterStemmer
from sklearn.preprocessing import normalize
from textblob import *
stemmer = PorterStemmer()

with open("count_vectorizer.pickle") as f : vectorizer =  pickle.load(f)
with open("annotated_data.pickle") as f : annotated_data = pickle.load(f)

words = { "affective" : ["happy", "sad", "love", "anxiety", "good", "hate", "depressed", "bad",
                        "anxious", "lonely"],
         "social" : ["family", "friends", "people", "person", "parents", "wife", "partner",
                     "husband", "relationship", "girlfriend", "boyfriend", "social", "crush", "gf", "ex", "bf"],
         "temporal" : ["time", "day", "years", "months"],
         "work" : ["life", "school", "work", "job"],
         "cognition" : ["hard", "lot", "like", "know", "think", "see", "felt", "wanted", "thought", "saw"],
         "inhibition" : ["avoid", "deny", "safe", "never", "quit"],
         "social media" : ["facebook", "social media", "snapchat", "twitter", "instagram", "social network",
                           "follow" "friended",
                           "retweet", "tweet", "insta", "follower", "hashtag", "tag", "messenger"],
         "deletion" : ["delete", "deleted", "deactivate","deactivated"]}

stemmed_words = {}
for key, value in words.items(): stemmed_words[key] = [stemmer.stem(term) for term in value]

LDA_makers = {}
for file in [file for file in os.listdir(os.getcwd()) if "LDA" in file and "topics" in file]:
    topic_count = int(file.split("_")[1])
    with open(file) as f : LDA_makers[topic_count] = pickle.load(f)


for submission in annotated_data:
    submission["tokens"] = re.split(r'[^a-zA-Z0-9]',submission["submission_body"].lower())
    for category in words:
        count = 0
        for i,word in enumerate(words[category]):
            if " " in word:
                count += submission["submission_body"].lower().count(word)
            else:
                count += submission["tokens"].count(stemmed_words[category][i])
        submission[category + ' keyword count'] = count
        submission_tb = textblob.TextBlob(submission["submission_body"])
        submission["sentiment"] = submission_tb.sentiment[0]
        submission["subjectivity"] = submission_tb.sentiment[1]
        submission["reading_level"] = textstat.flesch_kincaid_grade(submission["submission_body"])
        count_vector = vectorizer.transform([submission["submission_body"]])
        submission["document_unit_vector"] = normalize(count_vector, norm = "max")
        for topic_count, LDA_maker in LDA_makers.items():
            submission["LDA %d topics" % (topic_count,)] = LDA_maker.transform(count_vector)




    # Modification : Kriti
    # Added this code for adding a new feature: Part of speech tagging as an input to the random forest classifier.
    # For Part of Speech Tagging


    blob = TextBlob(submission["submission_body"])
    nounCount =0
    personalPronounCount = 0
    possesivePronounCount = 0
    adverbCount = 0
    verbCount = 0
    for word, tag in blob.tags:
        if tag == 'NN' or tag == 'NNS':
            nounCount += 1
        if tag == 'PRP':
            personalPronounCount += 1
        if tag == 'PRP$':
            possesivePronounCount += 1
        if tag == 'RB' or tag == 'RBR' or tag == 'RBS':
            adverbCount += 1
        if tag == 'VB' or tag == 'VBD' or tag == 'VBG' or tag == 'VBN' or tag == 'VBN' or tag == 'VBP' or tag == 'VBZ':
            verbCount += 1

        submission["noun_count"] = nounCount
        submission["verb_count"] = verbCount
        submission["adverb_count"] = adverbCount
        submission["personal_pronoun"] = personalPronounCount
        submission["possessive_pronoun"] = possesivePronounCount
    # End

with open("annotated_data_with_features_v3.pickle","w") as f : pickle.dump(annotated_data,f)