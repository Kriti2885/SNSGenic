"""
@Author : John Gann

@Modified : Kriti Upadhyaya
Mentioned tags in the program for my part.

@Dependencies:
pip install sklearn

The function convert the features and label data into vectors. Then test, train, split into a 80:20 ratio.
The program then implements random forest algorithm and performs evaluation.
"""

import cPickle as pickle
from random import shuffle
from random import choice as rchoice
from sklearn.metrics import precision_recall_fscore_support
from sklearn.ensemble import RandomForestClassifier


def vectorize(submission, LDA_topics):
    vector = list(submission["LDA %d topics" % LDA_topics][0])
    keyword_count_keys = [key for key in submission.keys() if "keyword count" in key]
    keyword_count_keys.sort()
    vector += [submission[key] for key in keyword_count_keys]
    vector.append(submission["sentiment"])
    vector.append(submission["subjectivity"])
    vector.append(submission["reading_level"])
    vector.append(len(submission["submission_body"]))




    # Modifications made for more features to be added in feature set.
    # Start
    vector.append(submission["verb_count"])
    vector.append(submission["adverb_count"])
    vector.append(submission["noun_count"])
    vector.append(submission["personal_pronoun"])
    vector.append(submission["possessive_pronoun"])
    # End

    return map(float,vector)


def stratifiedTestTrainSplit(train_test_ratio,data):
    relevant = []
    irrelevant = []
    for entry in data:
        if entry["relevant"]:
            relevant.append(entry)
        else:
            irrelevant.append(entry)
    shuffle(relevant)
    shuffle(irrelevant)
    num_train_relevant = int(float(train_test_ratio) / (1 + train_test_ratio) * len(relevant))
    num_train_irrelevant = int(float(train_test_ratio) / (1 + train_test_ratio) * len(irrelevant))
    train = relevant[:num_train_relevant] + irrelevant[:num_train_irrelevant]
    test = relevant[num_train_relevant:] + irrelevant[num_train_irrelevant:]
    return train, test


with open("annotated_data_with_features_v3.pickle") as f: data = pickle.load(f)

training_data, testing_data = stratifiedTestTrainSplit(4,data)
classifications = [1 if entry["relevant"] else 0 for entry in data]
randResults = [rchoice(classifications) for item in range(len(data))]
print precision_recall_fscore_support(classifications,randResults,beta=1.0,average="binary")
for LDA_topic_count in range(3,21):
    train_X = [vectorize(entry, LDA_topic_count) for entry in training_data]
    train_y = [1 if entry["relevant"] else 0 for entry in training_data]
    test_X = [vectorize(entry, LDA_topic_count) for entry in testing_data]
    test_y = [1 if entry["relevant"] else 0 for entry in testing_data]
    clf = RandomForestClassifier(random_state=0, n_estimators=1000)
    clf.fit(train_X,train_y)
    pred_y = clf.predict(test_X)
    precision, recall, F1, _ = list(precision_recall_fscore_support(test_y, pred_y, beta=1.0))
    macPrecision, macRecall, macF1, _ = list(precision_recall_fscore_support(test_y, pred_y, beta=1.0, average = "macro"))
    micPrecision, micRecall, micF1, _ = list(precision_recall_fscore_support(test_y, pred_y, beta=1.0, average = "micro"))
    feature_importances = clf.feature_importances_
    print "\n"
    print "LDA topics: %d " % (LDA_topic_count,)
    print "______________"
    print "Precision: %f: " +str(precision)
    print "Recall: %f: "+str(recall)
    print "F1: %f: " +str(F1)
    print "Macro Precision: %f: " +str(macPrecision)
    print "Macro Recall: %f: "+str(macRecall)
    print "Macro F1: %f: " +str(macF1)
    print
    print "Micro Precision: %f: " +str(micPrecision)
    print "Micro Recall: %f: "+str(micRecall)
    print "Micro F1: %f: " +str(micF1)
    print

    print
    print "LDA feature importances: " + str(feature_importances[:LDA_topic_count])
    for i,key in enumerate(sorted([key for key in data[0] if "keyword" in key])):
        print key + " feature importance: " + str(feature_importances[LDA_topic_count + i])
    print "Sentiment feature importance: " + str(feature_importances[LDA_topic_count + i + 1])
    print "Subjectivity feature importance: " + str(feature_importances[LDA_topic_count + i + 2])
    print "Reading level feature importance: " + str(feature_importances[LDA_topic_count + i + 3])
    print "Submission length feature importance: " + str(feature_importances[LDA_topic_count + i + 4])





    # Modifications made in the program
    # Start
    print "Verb Count feature importance: " + str(feature_importances[LDA_topic_count + i + 5])
    print "Adverb Count feature importance: " + str(feature_importances[LDA_topic_count + i + 6])
    print "Noun count feature importance: " + str(feature_importances[LDA_topic_count + i + 7])
    print "Personal Pronoun Count feature importance: " + str(feature_importances[LDA_topic_count + i + 8])
    print "Possesive Pronoun Count feature importance: " + str(feature_importances[LDA_topic_count + i + 9])
    # End

