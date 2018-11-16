import awsdora_connect as awsdora
import pickle
import time
import webofscience as WOS
import graph_analytics as gAnalytics
import tablib 
import numpy as np
from sklearn import svm
from sklearn.metrics import accuracy_score

db = awsdora.connect_to_awsdora()
paperCollection = db.collection('Paper')
edgesCollection = db.collection('Edges')
# Get the AQL API wrapper
aql = db.aql

#initialize features_matrix and labels
features_matrix = []
labels = []

f = open("kwd_records3_labeled.xls","rb")
labeled_set = tablib.Dataset()
labeled_set.xls = open("kwd_records3_labeled.xls",'rb').read()
for sample in labeled_set:
	if sample[9] != '':
		label = sample[9]
		features = [float(value) for value in sample[5:9]]

		if label == 'n':
			labels.append(0)
		elif label == 'y':
			labels.append(1)
		features_matrix.append(list(features))
model = svm.SVC()
model.fit(features_matrix,labels)
for sample in labeled_set:
	if sample[9] != '':
		features = [float(value) for value in sample[5:9]]
		print('test sample: ' + sample[0])
		#print('\tfeatures: ' + str(features))
		prediction = model.predict([features])
		print('\tclassified as: ' + str(prediction))
		print('\tactual classifcation : ' + str(sample[9]))

test_set = tablib.Dataset()
test_set.xls = open("kwd_records_2108326722_labeled.xls",'rb').read()
test_features_matrix = []
test_labels = []
for sample in labeled_set:
	if sample[9] != '':
		label = sample[9]
		features = [float(value) for value in sample[5:9]]

		if label == 'n':
			test_labels.append(0)
		elif label == 'y':
			test_labels.append(1)
		test_features_matrix.append(list(features))
predictions = model.predict(test_features_matrix)
print(accuracy_score(test_labels,predictions))



