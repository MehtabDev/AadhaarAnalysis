package com.spark.analysis.process;

import com.google.inject.internal.util.$Function;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;

/**
 * created this class to find the top 5 aadhar generator agency
 * data from Kaggle
 */
public class AadharGeneratedAgency {

    public Dataset<Row> transform (Dataset<Row> origData){
        origData = origData.groupBy("Enrolment Agency").agg(functions.sum("Aadhaar generated").as("no. of aadhar")).orderBy(functions.sum("Aadhaar generated").desc()).limit(5);
        return origData;
    }
}
