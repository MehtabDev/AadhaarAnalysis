package com.spark.analysis.flow;

import com.spark.analysis.job.SparkDataJob;
import org.apache.spark.sql.SparkSession;

import java.io.UnsupportedEncodingException;

public class SparkDataFlow {

    public SparkDataFlow(SparkSession sparkSession) throws UnsupportedEncodingException {
        new SparkDataJob(sparkSession).extractor();
    }

}
