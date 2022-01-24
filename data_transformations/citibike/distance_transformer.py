from pyspark.sql import SparkSession, DataFrame, functions as F

METERS_PER_FOOT = 0.3048
FEET_PER_MILE = 5280
EARTH_RADIUS_IN_METERS = 6371e3
METERS_PER_MILE = METERS_PER_FOOT * FEET_PER_MILE


def compute_distance(_spark: SparkSession, dataframe: DataFrame) -> DataFrame:

    key_columns = ["starttime", "tripduration", "start_station_id"]

    coordinate_columns = [
        "start_station_latitude",
        "end_station_latitude",
        "start_station_longitude",
        "end_station_longitude",
    ]

    required_columns = key_columns + coordinate_columns

    tmp_dataframe = (
        dataframe.select(required_columns)
        .withColumn(
            "delta_phi",
            F.radians(F.col("end_station_latitude") - F.col("start_station_latitude")),
        )
        .withColumn(
            "delta_lambda",
            F.radians(
                F.col("end_station_longitude") - F.col("start_station_longitude")
            ),
        )
        .withColumn("phi_1", F.radians(F.col("start_station_latitude")))
        .withColumn("phi_2", F.radians(F.col("end_station_latitude")))
    )

    tmp_dataframe = tmp_dataframe.withColumn(
        "alpha",
        F.sin(F.col("delta_phi") / 2.0) ** 2
        + F.cos(F.col("phi_1"))
        * F.cos(F.col("phi_2"))
        * (F.sin(F.col("delta_lambda") / 2.0) ** 2),
    )

    tmp_dataframe = tmp_dataframe.withColumn(
        "distance_scalar",
        2 * F.atan2(F.sqrt(F.col("alpha")), F.sqrt(1 - F.col("alpha"))),
    )

    earth_radius_in_miles = EARTH_RADIUS_IN_METERS / METERS_PER_MILE

    miles_df = tmp_dataframe.withColumn(
        "distance", F.round(earth_radius_in_miles * F.col("distance_scalar"), scale=2)
    ).select(key_columns + ["distance"])

    return dataframe.join(miles_df, on=key_columns).select(
        dataframe.columns + ["distance"]
    )


def run(
    spark: SparkSession, input_dataset_path: str, transformed_dataset_path: str
) -> None:
    input_dataset = spark.read.parquet(input_dataset_path)
    input_dataset.show()

    dataset_with_distances = compute_distance(spark, input_dataset)
    dataset_with_distances.show()

    dataset_with_distances.write.parquet(transformed_dataset_path, mode="append")
