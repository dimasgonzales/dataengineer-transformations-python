import logging

from pyspark.sql import SparkSession, DataFrame, functions as F
from pyspark.ml import feature


def run(spark: SparkSession, input_path: str, output_path: str) -> None:
    logging.info("Reading text file from: %s", input_path)
    input_df = spark.read.text(input_path)

    tokenized_df = _tokenize_dataframe(input_df)
    token_count_df = _count_tokens(spark, tokenized_df)

    logging.info("Writing csv to directory: %s", output_path)

    token_count_df.coalesce(1).write.csv(output_path, header=True)


def _tokenize_dataframe(input_df: DataFrame) -> DataFrame:

    cleaned_df = input_df.withColumn(
        "value",
        F.regexp_replace("value", r'[-;.,"\\]', " "),
    )

    tokenizer = feature.Tokenizer(inputCol="value", outputCol="words")
    tokenized_df = tokenizer.transform(cleaned_df)

    return tokenized_df


def _count_tokens(spark: SparkSession, tokenized_df: DataFrame) -> DataFrame:
    token_cnt_map = tokenized_df.rdd.map(__token_count_map).reduce(
        __reduce_token_countmap
    )

    token_cnt_list = []
    for token_name, token_count in token_cnt_map.items():
        token_cnt_list.append([token_name, token_count])
    return spark.createDataFrame(token_cnt_list, ["word", "count"]).orderBy("word")


def __token_count_map(row):
    count_map = {}

    for token in row["words"]:
        if len(token) == 0:
            continue

        if token not in count_map:
            count_map.update({token: 1})
        else:
            count_map[token] += 1
    return count_map


def __reduce_token_countmap(left_map, right_map):
    reduced_count_map = {}

    for key in set(list(left_map.keys()) + list(right_map.keys())):
        if key in left_map and key in right_map:
            reduced_count_map.update({key: left_map[key] + right_map[key]})
        elif key in left_map:
            reduced_count_map.update({key: left_map[key]})
        else:
            reduced_count_map.update({key: right_map[key]})

    return reduced_count_map
