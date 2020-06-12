package com.spark.analysis.job;

import com.spark.analysis.extractor.SparkDataExtractor;
import com.spark.analysis.row.Rows;
import com.spark.analysis.row.RowsProvider;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.io.UnsupportedEncodingException;
import java.net.URL;
import java.net.URLDecoder;

public class SparkDataJob {

    private SparkSession sparkSession;

    public SparkDataJob(SparkSession sparkSession){
        this.sparkSession = sparkSession;
    }

    public void extractor() throws UnsupportedEncodingException {

        Rows extractor = new SparkDataExtractor(sparkSession, getData(sparkSession, "abc.csv")).extract();

    }

    private Rows getData(SparkSession sparkSession, String relativePath) throws UnsupportedEncodingException {
        URL resource = ClassLoader.getSystemResource(relativePath);
        String path = URLDecoder.decode(resource.getFile(), "UTF-8");
        Dataset<Row> datasetMock2 = sparkSession.read()
                .format("org.apache.spark.sql.execution.datasources.csv.CSVFileFormat")
                .option("delimiter", ",")
                .option("header","true")
                .option("inferSchema", false).load(path);
        datasetMock2.show();
        return RowsProvider.provide(datasetMock2);
    }
}
