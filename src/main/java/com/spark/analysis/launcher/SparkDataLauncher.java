package com.spark.analysis.launcher;

import com.spark.analysis.flow.SparkDataFlow;
import com.spark.analysis.helper.SparkSessionProvider;
import org.apache.log4j.Logger;
import org.apache.spark.sql.SparkSession;

import java.io.UnsupportedEncodingException;

/**
 * Launcher of project, means creating SparkSession here for project.
 */
public class SparkDataLauncher {
    Logger logger = Logger.getLogger(this.getClass());
    private static final String EXECUTION_NAME = "FactUserBillingActivity";
    private SparkSession sparkSession;

    public SparkDataLauncher(){
        this.sparkSession = SparkSessionProvider.provide(EXECUTION_NAME);
    }

    public void launch() throws UnsupportedEncodingException {
        new SparkDataFlow(sparkSession);
        logger.info("Process finished ok");
//        this.sparkSession.close();

    }
}
