# Importing the required libraries.
import numpy as np
import pandas as pd
data = pd.read_csv('project_data.csv',sep=";")
data = data.drop(columns = ["id"])
outlier = ((data["ap_hi"]>200) | (data["ap_lo"]>180) | (data["ap_lo"]<50) | (data["ap_hi"]<=80) | (data["height"]<=100) | (data["weight"]<=28) )
data = data[~outlier]
data["bmi"] = data["weight"]/ (data["height"]/100)**2
# Detecting Genders
a = data[data["gender"]==1]["height"].mean()
b = data[data["gender"]==2]["height"].mean()
if a > b:
    gender = "male"
    gender2 = "female"
else:
    gender = "female"
    gender2 = "male"
data["gender"] = data["gender"] % 2
X = data.drop(columns = ['cardio'])
y = data['cardio']
from sklearn.preprocessing import MinMaxScaler
scalar=MinMaxScaler()
x_scaled=scalar.fit_transform(X)
from sklearn.tree import DecisionTreeClassifier
from sklearn.ensemble import RandomForestClassifier
from sklearn.neighbors import KNeighborsClassifier
from sklearn.svm import SVC
from sklearn.model_selection import GridSearchCV,RandomizedSearchCV,train_test_split
from sklearn.metrics import accuracy_score,confusion_matrix,f1_score,roc_curve, roc_auc_score
X_train, X_test, y_train, y_test = train_test_split(x_scaled, y, test_size = 0.30, random_state = 9)
dtc = DecisionTreeClassifier()
ran = RandomForestClassifier(n_estimators=90)
knn = KNeighborsClassifier(n_neighbors=79)
svm = SVC(random_state=6)
models = {"SVM" : svm}

scores= { }
for key, value in models.items():    
    model = value
    model.fit(X_train, y_train)
    scores[key] = model.score(X_test, y_test)
print("started")
from kafka import KafkaConsumer
from json import loads
import json
import pandas as pd
from kafka import KafkaProducer
from pykafka import KafkaClient
consumer = KafkaConsumer('input_topic',bootstrap_servers=['kafka:9092'])
client = KafkaClient(hosts="kafka:9092")
topic = client.topics['output_topic']
producer = topic.get_sync_producer()
for message in consumer:
    df = pd.read_json(message.value, orient="index")
    df = df.transpose()
    dftmp = df
    dftmp = dftmp.drop(columns = ["id"])
    dftmp = dftmp.drop(columns = ['cardio'])
    dftmp["bmi"] = dftmp["weight"]/ (dftmp["height"]/100)**2
    dftmp["gender"] = dftmp["gender"] % 2
    message_x_scaled=scalar.transform(dftmp)
    
    predicted_svc=svm.predict(message_x_scaled)
    
    
    if(predicted_svc==1):
        df["prediction"] = "abnormal"
    else:
        df["prediction"] = "normal"
    
    result = df.to_json(orient="index")
    producer.produce(result.encode('ascii'))
    
    
    
