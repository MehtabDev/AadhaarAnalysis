package com.spark.analysis.helper;

import org.apache.spark.sql.SparkSession;

/**
 * created this class to create SparkSession object by using required configuration.
 */
public class SparkSessionProvider {

    public static SparkSession provide(String appName) {

        SparkSession.Builder builder = SparkSession.builder();
        builder = builder.appName(appName)
                .master("local");
        return builder.getOrCreate(); // it will return SparkSession object
    }
}
