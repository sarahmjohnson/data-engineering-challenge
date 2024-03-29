<p align="center"><a href="https://novoic.com"><img src="https://assets.novoic.com/logo_320px.png" alt="Novoic logo" width="160"/></a></p>

# Novoic data engineering challenge

## How to

Clone the repo and run:
```
pip install -r requirements.txt
python -m spacy download en_core_web_sm
```

The raw data should be in this format:
```
data-engineering-challenge
    data
        audio
            *.wav
        text
            *.xml
        metadata.csv
```

Run to generate all 3 datasets:
```
python main.py
```

You will find the training_data under the folder:
```
data-engineering-challenge
    training_data
```

## Things to note

Used `black` to format according to PEP8.

Created `.gitignore` to disclude the PI data. Normally, the `.gitignore` should also include `/data` and `/training_data` and data should only be sent through airdrop or uploaded to private s3 and downloaded. However, since it is dummy data, I've included them in the github repo. 

With more time, I would dockerize and create unit tests.

## QUESTIONS

How would you deploy this repository on a Kubernetes cluster?

- I would follow the documentation here: https://sparkbyexamples.com/spark/spark-submit-command/.
- Here is the command that would submit the app:

```
./bin/spark-submit \
  --master <master-url> \
  --deploy-mode <deploy-mode> \
  --conf <key<=<value> \
  --driver-memory <value>g \
  --executor-memory <value>g \
  --executor-cores <number of cores>  \
  --jars  <comma separated dependencies>
  --class <main-class> \
  <application-jar> \
  [application-arguments]
  ```

  - `deploy-mode` would be `cluster` so that the spark driver runs on one of the nodes in the cluster, `master` would be `kubernetes`, and `jars` would include `spark-xml`. 


Assume we now are using this repository as part of a product that we have deployed. How would you ensure that we can stream the data preprocessing? What technologies would you use? 

- I would use Apache Kafka. The spark application would fetch data from the Kafka topic. I would edit the code in `main.py` that calls a function and processes a single folder of data. Instead, it would constantly stream in the data and process it in batches. I find this is a good guide of how to implement streaming with Kafka and pyspark: https://spark.apache.org/docs/latest/streaming-programming-guide.html.