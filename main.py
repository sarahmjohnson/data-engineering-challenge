import os
import pandas as pd
import numpy as np
import json
from typing import Dict
from scipy.io import wavfile
from pyspark import SparkContext
from pyspark.sql import SQLContext, DataFrame, SparkSession
from pyspark.sql.functions import udf, split, collect_set
from pyspark.sql.types import StringType, ArrayType, FloatType, StructType, StructField
from utils import (
    text_to_features,
    audio_to_features,
)

XML_DIR = "data/text"
WAV_DIR = "data/audio"
TRAINING_DATA_PATH = "training_data"

# create spark session and load in spark-xml package
spark = (
    SparkSession.builder.appName("Data Engineering Challenge - Sarah Johnson")
    .config("spark.jars.packages", "com.databricks:spark-xml_2.12:0.10.0")
    .getOrCreate()
)


def transform_audio() -> DataFrame:
    """
    Create audio dataframe from wavs
    Preprocess and transform into better format

    It was tricky to import the wav files because they are np.ndarray type
    and pyspark dataframe does not support np.ndarray on import.

    I ultimately decided to create an rdd, convert the np.ndarray to json,
    transform the rdd to dataframe, and then convert the json back to
    np.ndarray.

    Args:
        None
    Returns:
        DataFrame (
            speakerid: str
            wav_features_array: DataFrame
        )
    """
    print("INFO: Getting audio dataframe...")

    def read_chunk(path_to_wav: str) -> (str, json):
        """
        Args:
            path_to_wav: str
        Returns:
            segmentid: str
            wav_features_json: json
        """
        # e.g. path_to_wav: S001.wav --> segmentid: S001
        segmentid = path_to_wav.split(".")[0].split("/")[2]

        # use scipy.wavfile to read in wav_array
        wav_data = wavfile.read(path_to_wav)
        wav_array = list(wav_data)[1]

        # convert raw array to feature array
        wav_features_array = audio_to_features(wav_array)

        # convert feature array to json in order to successfully import to pyspark df later
        wav_features_json = json.dumps(wav_features_array.tolist())
        return segmentid, wav_features_json

    # parallelize streaming of wav files
    partitions = [os.path.join(WAV_DIR, x) for x in os.listdir(WAV_DIR)]
    audio_rdd = spark.sparkContext.parallelize(partitions, len(partitions)).map(
        read_chunk
    )

    # convert rdd to df
    audioSchema = StructType(
        [
            StructField("speakerid", StringType(), True),
            StructField("wav_features_array", StringType(), True),
        ]
    )
    audio_df = spark.createDataFrame(data=audio_rdd, schema=audioSchema)

    def json_to_np_array(x: json) -> np.ndarray:
        """
        Args:
            x: json
        Returns:
            np.ndarray
        """
        return np.array(json.loads(x))

    # convert json of wav feature arrays to np.ndarray types
    udf_json_to_np_array = udf(
        lambda x: json_to_np_array(x).tolist(), ArrayType(FloatType())
    )
    audio_df = audio_df.withColumn(
        "wav_features_array", udf_json_to_np_array(audio_df.wav_features_array)
    )
    return audio_df


def transform_text() -> DataFrame:
    """
    Create text dataframe from xmls
    Preprocess and transform into better format

    Args:
        None
    Returns:
        DataFrame (
            speakerid: str
            wav_features_array: DataFrame
        )
    """
    print("INFO: Getting text dataframe...")
    text_df = (
        spark.read.format("xml")
        .option("rootTag", "root")
        .option("rowTag", "segment")
        .load(XML_DIR)
    )

    # e.g. _segmentid: S001_0 --> speakerid: S001 for inner join later
    text_df = text_df.withColumn("speakerid", split("_segmentid", "_").getItem(0))
    text_df = text_df.drop("_tend", "_tstart", "_segmentid")

    def convert_to_sentence(w: list) -> str:
        """
        Args:
            w: List(
                _VALUE: str,
                _speakerid: str
            )
        Returns:
            sentence: str
        """
        sentence = ""
        for sentence_row in w:
            word = sentence_row["_VALUE"]
            sentence += f" {word}"
        return sentence

    # convert word strings into complete sentence string
    udf_convert_to_sentence = udf(lambda x: convert_to_sentence(x), StringType())
    text_df = text_df.withColumn("w", udf_convert_to_sentence(text_df.w))

    # convert raw array to feature array
    udf_text_to_features = udf(
        lambda x: text_to_features(x).tolist(), ArrayType(FloatType())
    )
    text_df = text_df.withColumn("text_feature_arrays", udf_text_to_features(text_df.w))
    text_df = text_df.drop("w")

    # group by speakerid to gather text_feature_arrays associated with the same speakerid for easier access in training
    text_df = text_df.groupby("speakerid").agg(collect_set("text_feature_arrays"))
    return text_df


def sort_to_groups(
    type_classification: str, csv_df: DataFrame
) -> (DataFrame, DataFrame):
    """
    Depending on the input string, the data will be sorted into group_0 or group_1 for training
    More types could be added here if someone wanted a new training dataset

    Args:
        type_classification: str
        csv_df: str
    Returns:
        DataFrame (depends on type_classification)
    """
    print("INFO: Sorting to groups...")
    # Classification of AD (Alzheimer's disease; `status = 1`) vs HC (healthy control, i.e. `status = 0`).
    if type_classification == "basic_classification":
        csv_df = csv_df.drop("age", "gender", "mmse", "set")
        csv_df = csv_df.withColumn("speakerid", split("speakerid", "_").getItem(1))
        csv_df_group_0 = csv_df.filter(csv_df.ad_status == 0)
        csv_df_group_1 = csv_df.filter(csv_df.ad_status == 1)
        return csv_df_group_0, csv_df_group_1

    # Classification of AD-MCI (mild cognitive impairment due to Alzheimer's disease; `status = 1 AND mmse > 26`) vs HC.
    elif type_classification == "ad_mci_classification":
        csv_df = csv_df.drop("age", "gender", "set")
        csv_df = csv_df.withColumn("speakerid", split("speakerid", "_").getItem(1))
        csv_df_group_0 = csv_df.filter(csv_df.ad_status == 0)
        csv_df_group_1 = csv_df.filter((csv_df.ad_status == 1) & (csv_df.mmse > 26))
        return csv_df_group_0, csv_df_group_1

    # Classification of AD vs HC for patients in a certain age range. For example classification of AD vs HC for patients who are less than 60 years old (a loose proxy for preclinical Alzheimer’s).
    elif type_classification == "ad_hc_younger_60_classification":
        csv_df = csv_df.drop("gender", "mmse", "set")
        csv_df = csv_df.withColumn("speakerid", split("speakerid", "_").getItem(1))
        csv_df_group_0 = csv_df.filter((csv_df.ad_status == 0) & (csv_df.age < 60))
        csv_df_group_1 = csv_df.filter((csv_df.ad_status == 1) & (csv_df.age < 60))
        return csv_df_group_0, csv_df_group_1


def create_dataset(type_classification: str) -> Dict:
    """
    Create dataset for training based on the type of classification

    Args:
        type_classification: str
    Returns:
        Dict (
            group_0: DataFrame,
            group_1: DataFrame,
            path_to_group_0: str,
            path_to_group_1: str,
        )
    """
    print(f"INFO: Starting {type_classification} dataset creation...")

    # create dataframes for csv, audio, and text files
    csv_df = spark.read.csv("data/metadata.csv", header="True", inferSchema="True")
    csv_df_group_0, csv_df_group_1 = sort_to_groups(type_classification, csv_df)
    audio_df = transform_audio()
    text_df = transform_text()

    # perform inner joins based off of the first key (segmentid)
    print("INFO: Performing inner joins for group_0...")
    group_0 = csv_df_group_0.join(text_df, "speakerid")
    group_0 = group_0.join(audio_df, "speakerid")
    pd_df_group_0 = group_0.toPandas()
    print("INFO: Performing inner joins for group_1...")
    group_1 = csv_df_group_1.join(text_df, "speakerid")
    group_1 = group_1.join(audio_df, "speakerid")
    group_1_pd_df = group_1.toPandas()

    # export to json for easy downstream processing
    print("INFO: Exporting data to jsons...")
    output_folder = os.path.join(TRAINING_DATA_PATH, type_classification)
    if not os.path.exists(output_folder):
        os.makedirs(output_folder)
    path_to_group_0 = os.path.join(output_folder, "group_0.json")
    path_to_group_1 = os.path.join(output_folder, "group_1.json")

    pd_df_group_0.to_json(path_to_group_0, orient="index", indent=2)
    group_1_pd_df.to_json(path_to_group_1, orient="index", indent=2)

    print("INFO: Completed Dataset")
    return {
        "group_0": group_0,
        "group_1": group_1,
        "path_to_group_0": path_to_group_0,
        "path_to_group_1": path_to_group_1,
    }


result = create_dataset("basic_classification")
print(result)

result = create_dataset("ad_mci_classification")
print(result)

result = create_dataset("ad_hc_younger_60_classification")
print(result)
