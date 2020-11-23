<p align="center"><a href="https://novoic.com"><img src="https://assets.novoic.com/logo_320px.png" alt="Novoic logo" width="160"/></a></p>

# Novoic data engineering challenge

"""
Used `black` to format according to PEP8
Created `.gitignore` to disclude the PI data - shouldn't push any data to github ever - should only be sent through airdrop, upload to private s3 and download, and should document what has been sent and where 

With more time, I would dockerize and create unit tests.


- How would you deploy this repository on a Kubernetes cluster?

I would follow the documentation here: https://sparkbyexamples.com/spark/spark-submit-command/
Here is the command that would submit the app:

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
  [application-arguments]```

  I would include the `spark-xml` jar and the `deploy-mode` would be `cluster` so that the spark driver runs on one of the nodes in the cluster. The `master` option would be `kubernetes`. 


- Assume we now are using this repository as part of a product that we have deployed. How would you ensure that we can stream the data preprocessing? What technologies would you use? 

I would use Apache Kafka. The spark application would fetch data from the Kafka topic. I would edit the code in `main.py` to instead of call a function like I do now to process a folder of data, constantly stream in the xml and wav files. 