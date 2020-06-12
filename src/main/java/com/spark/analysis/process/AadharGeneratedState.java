package com.spark.analysis.process;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;

/**
 * created this class to find the number of aadhar generated in each state
 * data from Kaggle
 */
public class AadharGeneratedState {

    public Dataset<Row> transform(Dataset<Row> aadharOrigData){
        // groupby using State column, sum Aadhaar genarated column which is 1 or 0.
        // rename the sum("Aadhaar generated") as you want either using as or alias function
        aadharOrigData = aadharOrigData.groupBy("State").agg(functions.sum("Aadhaar generated").as("No. of aadhar")).orderBy("state");

        return aadharOrigData;

    }
}
