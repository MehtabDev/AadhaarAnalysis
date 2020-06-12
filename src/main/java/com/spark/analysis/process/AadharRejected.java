package com.spark.analysis.process;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;

/**
 * created this class to number of aadhar rejected in a particular state using where()
 * data from Kaggle
 */
public class AadharRejected {

    public Dataset<Row> transform(Dataset<Row> origData){
        origData = origData.groupBy("State").agg(functions.sum("Enrolment Rejected").as("rejected")).where(origData.col("State").equalTo("Uttar Pradesh"));
        return origData;

    }
}
