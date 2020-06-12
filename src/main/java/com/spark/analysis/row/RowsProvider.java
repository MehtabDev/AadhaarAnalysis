package com.spark.analysis.row;

import org.apache.spark.sql.Dataset;

public class RowsProvider {

    public static <T> Rows provide(final Dataset<T> dataset) {

        return new Rows() {

            @SuppressWarnings("unchecked")
            public Dataset<T> dataset() {
                return dataset;
            }
        };
    }

    public static <T> Rows provide(final Rows rows) {

        return new Rows() {

            @SuppressWarnings("unchecked")
            public Dataset<T> dataset() {
                return rows.dataset();
            }
        };
    }
}
