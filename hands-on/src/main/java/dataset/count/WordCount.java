package dataset.count;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;

public class WordCount
{
    private static final String DIR = "/Users/yjkim-studio/src/flink/hands-on/data/";
    private static final String INPUT = DIR + "word/wc.txt";
    private static final String OUTPUT = DIR + "output/wcResult.csv";

    public static void main(String[] args) throws Exception {
        // Set up the execution environment
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        final ParameterTool params = ParameterTool.fromArgs(args);

        // Make parameters available in the web interface
        env.getConfig().setGlobalJobParameters(params);

        // Read the text file from given input path
        DataSet<String> text = env.readTextFile(INPUT);

        // Filter all the names starting with N
        DataSet<String> filtered = text.filter(new FilterFunction<String>() {
            public boolean filter(String value)
            {
                return value.startsWith("N");
            }
        });

        // Returns a tuple of (name, 1)
        DataSet<Tuple2<String, Integer>> tokenized = filtered.map(new Tokenizer());

        // Group by tuple field "0" and sum up tuple field "1"
        DataSet<Tuple2<String, Integer>> counts = tokenized.groupBy(0).sum(1);
        // UnsortedGrouping<Tuple2<String, Integer>> counts1 = tokenized.groupBy(0);
        // DataSet<Tuple2<String, Integer>> counts = counts1.sum(1);

        // save the result
        counts.writeAsCsv(OUTPUT, "\n", " ");

        // execute program
        env.execute("WordCount Example");
    }

    private static final class Tokenizer implements MapFunction<String, Tuple2<String, Integer>> {
        public Tuple2<String, Integer> map(String value) {
            return new Tuple2<String, Integer>(value, 1);
        }
    }
}