package com.spark.analysis.launcher;

/**
 * Java main class, project entry point, we can capture any number of arguments if required.
 */
public class SparkDataMain {
    public static void main(String[] args)  {
        try {
            new SparkDataLauncher().launch();
        }catch (Exception e){
            e.printStackTrace();
        }
    }
}
