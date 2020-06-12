package com.spark.analysis.row;

import org.apache.spark.sql.Dataset;

public interface Rows {
    <T> Dataset<T> dataset();
}
