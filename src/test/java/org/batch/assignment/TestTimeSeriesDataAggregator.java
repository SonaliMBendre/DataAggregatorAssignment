package org.batch.assignment;

import org.apache.commons.cli.Options;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.Test;

import static org.batch.assignment.CommonFields.APP_NAME;
import static org.junit.Assert.*;

public class TestTimeSeriesDataAggregator {
    TimeSeriesDataAggregator aggregator = new TimeSeriesDataAggregator();

    @Test
    public void testInit() {
        SparkSession spark = aggregator.init();
        assertEquals(APP_NAME, spark.sparkContext().appName());
    }

    @Test
    public void testParseArguments() {
        Throwable e = assertThrows(IllegalArgumentException.class,
                () -> {
                    aggregator.parseCommandLineArgs(new String[]{""});
                });
        assertTrue(e.getMessage().contains("Error parsing arguments!"));

        e = assertThrows(IllegalArgumentException.class,
                () -> {
                    aggregator.parseCommandLineArgs(null);
                });
        assertTrue(e.getMessage().contains("Error parsing arguments!"));
    }

    @Test
    public void testDefineCommandLineOptions() {
        Options options = aggregator.defineCommandLineOptions();
        assertEquals(4, options.getRequiredOptions().size());
    }

    @Test
    public void testEvaluate() {
        Throwable e = assertThrows(IllegalArgumentException.class,
                () -> {
                    aggregator.evaluate(aggregator.init());
                });
        assertTrue(e.getMessage().contains("Can not create a Path from a null string"));

        SparkSession spark = aggregator.init();
        aggregator.parseCommandLineArgs(new String[]{"--operationType", "min", "--windowDuration", "2 hours", "--input", "src\\test\\resources\\input\\data.csv", "--output", "src\\test\\resources\\outputDir"});
        Dataset<Row> result = aggregator.evaluate(spark);
        assertTrue(!result.isEmpty());
    }
}
