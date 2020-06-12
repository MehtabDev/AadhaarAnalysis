package com.spark.analysis.process;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;

/**
 *  In data we have one column as gender having M and F
 *  need to use UDF to bifurcate these 2 genders.
 */
public class GenderAnalysis {

    public Dataset<Row> transform(Dataset<Row> origData){

        // first we need to add male and female columns for filteration.
        origData = origData.withColumn("male",functions.when(origData.col("gender").equalTo("M"), "1").otherwise("0"));
        origData = origData.withColumn("female",functions.when(origData.col("gender").equalTo("F"), "1").otherwise("0"));
        origData = origData.groupBy("State").agg(functions.sum("male").as("no. of male"), functions.sum("female").as("no. of female")).orderBy("State");

        return origData;
    }

}
