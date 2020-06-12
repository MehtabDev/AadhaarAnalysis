package com.spark.analysis.extractor;


import com.spark.analysis.process.AadharGeneratedAgency;
import com.spark.analysis.process.AadharGeneratedState;
import com.spark.analysis.process.AadharRejected;
import com.spark.analysis.process.GenderAnalysis;
import com.spark.analysis.row.Rows;
import com.spark.analysis.row.RowsProvider;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class SparkDataExtractor {
    private SparkSession sparkSession;
    private Rows rows;


    public SparkDataExtractor(SparkSession sparkSession, Rows rows) {
        this.sparkSession = sparkSession;
        this.rows = rows;
    }


    public Rows extract() {
        Dataset<Row> originalDataset = rows.dataset();
        Dataset<Row> aadharState = new AadharGeneratedState().transform(originalDataset);
        Dataset<Row> aadharGender = new GenderAnalysis().transform(originalDataset);
        Dataset<Row> aadharAgency = new AadharGeneratedAgency().transform(originalDataset);
        Dataset<Row> aadharRejected = new AadharRejected().transform(originalDataset);

        aadharRejected.show();
        return RowsProvider.provide(aadharState);
    }

}
