package org.batch.assignment;

import org.apache.commons.cli.*;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.StructType;

import static org.batch.assignment.CommonFields.*;

/**
 * This class executes a batch job that will aggregate the data by time bucket and metric in that timebucket.
 * For each time bucket and each metric in the bucket, it will
 * calculate the average, min or max value of the metric based on aggregate operationType passed
 */

public class TimeSeriesDataAggregator {
    private String input, output, windowDuration, operationType;

    public static void main(String[] args) {
        TimeSeriesDataAggregator aggregator = new TimeSeriesDataAggregator();

        SparkSession spark = aggregator.init();
        aggregator.parseCommandLineArgs(args);
        Dataset<Row> result = aggregator.evaluate(spark);
        aggregator.saveOutput(result);

        spark.stop();
    }

    /**
     * This method will create sparkSession object with master set to local[*]
     *
     * @return SparkSession
     */
    protected SparkSession init() {
        SparkConf conf = new SparkConf();
        conf.set("spark.sql.session.timeZone", "UTC");
        return SparkSession.builder()
                .master("local[*]")
                .config(conf)
                .appName(APP_NAME)
                .getOrCreate();
    }

    /**
     * This method will define the Command Line Options required
     *
     * @return Options
     */
    protected Options defineCommandLineOptions() {
        Option operationType = new Option(OPERATION_TYPE, true, "Valid operation types : Avg, Min or Max");
        operationType.setRequired(true);
        Option windowDuration = new Option(WINDOW_DURATION, true, "window duration to aggregate data on");
        windowDuration.setRequired(true);
        Option input = new Option(INPUT, true, "Input path");
        input.setRequired(true);
        Option output = new Option(OUTPUT, true, "Output path");
        output.setRequired(true);

        Options options = new Options();
        options.addOption(operationType)
                .addOption(windowDuration)
                .addOption(input)
                .addOption(output);
        return options;
    }

    /**
     * This method will parse the command line arguments
     *
     * @param args {"--operationType","min","--windowDuration","2 hours", "--input", "inputDir", "--output", "outputDir"}
     */
    protected void parseCommandLineArgs(String[] args) {
        CommandLineParser parser = new BasicParser();
        Options options = defineCommandLineOptions();
        try {
            CommandLine commandLine = parser.parse(options, args);
            if (commandLine.hasOption(OPERATION_TYPE)) {
                operationType = commandLine.getOptionValue(OPERATION_TYPE);
            }
            if (commandLine.hasOption(WINDOW_DURATION)) {
                windowDuration = commandLine.getOptionValue(WINDOW_DURATION);
            }
            if (commandLine.hasOption(INPUT)) {
                input = commandLine.getOptionValue(INPUT);
            }
            if (commandLine.hasOption(OUTPUT)) {
                output = commandLine.getOptionValue(OUTPUT);
            }
        } catch (ParseException exception) {
            throw new IllegalArgumentException("Error parsing arguments! Error message: " + exception.getMessage());
        }
    }

    /**
     * This method reads the input csv file using the schema defined and based on the operationType and
     * windowDuration performs the aggregation (avg, min or max) on the input dataset
     *
     * @param spark SparkSession object created
     * @return Dataset<Row> result dataset which will be written to output dir
     */
    protected Dataset<Row> evaluate(SparkSession spark) {
        StructType schema = new StructType()
                .add(METRIC, "string")
                .add(VALUE, "double")
                .add(TIMESTAMP, "timestamp");

        Dataset<Row> dataset = spark.read()
                .option("mode", "DROPMALFORMED")
                .schema(schema).csv(input);

        Dataset<Row> result;
        String opType = operationType.toLowerCase();
        switch (opType) {
            case "avg":
                //Create windows/timebuckets based on the windowDuration passed, group by timebucket and then metric within that bucket
                // to aggregate the values
                result = dataset.groupBy(functions.window(functions.col(TIMESTAMP), windowDuration), functions.col(METRIC))
                        .avg(VALUE) //calculates the average of values for the grouped dataset
                        .withColumnRenamed("avg(Value)", opType + VALUE);
                result.show(false);
                break;
            case "min":
                result = dataset.groupBy(functions.window(functions.col(TIMESTAMP), windowDuration), functions.col(METRIC))
                        .min(VALUE)//calculates the min of values for the grouped dataset
                        .withColumnRenamed("min(Value)", opType + VALUE);
                break;
            case "max":
                result = dataset.groupBy(functions.window(functions.col(TIMESTAMP), windowDuration), functions.col(METRIC))
                        .max(VALUE)//calculates the max of values for the grouped dataset
                        .withColumnRenamed("max(Value)", opType + VALUE);
                break;
            default:
                throw new IllegalArgumentException("ERROR: Unrecognised operation type: " + opType);
        }
        return result;
    }

    /**
     * Converts the struct column type :
     * -- window: struct
     * |    |-- start: timestamp
     * |    |-- end: timestamp
     * to timebucket string column of format "window.start<->window.end
     * and then drops the struct column window.
     * The final output of format : timebucket, metric and value (avg, min or max) is then stored to output dir as csv
     *
     * @param dataset result Dataset
     */
    protected void saveOutput(Dataset<Row> dataset) {
        Dataset<Row> result = dataset.withColumn(TIME_BUCKET, functions.concat_ws(" -> ", functions.col("window.start"), functions.col("window.end")))
                .drop(functions.col("window"))
                .select(functions.col(TIME_BUCKET), functions.col(METRIC), functions.col(operationType.toLowerCase() + VALUE))
                .orderBy(TIME_BUCKET);

        result.write().option("header", "true").mode(SaveMode.Overwrite).csv(output);
    }
}