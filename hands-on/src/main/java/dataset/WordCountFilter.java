package main.java.dataset;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;

public class WordCountFilter {
    public static void main(String[] args) throws Exception {
        // Set up the environment
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        final ParameterTool params = ParameterTool.fromArgs(args);

        env.getConfig().setGlobalJobParameters(params);

        String inputPath = "data/wordCount/wc.txt";
        DataSet<String> text = env.readTextFile(inputPath);

        DataSet<String> filtered = text.filter(new MyFilter());

        DataSet<Tuple2<String, Integer>> tokenized = filtered.map(new Tokenizer());

    }

    public static final class MyFilter implements FilterFunction<String>{
        public boolean filter(String value) {
            return value.startsWith("N");
        }
    }

    public static final class Tokenizer implements MapFunction<String, Tuple2<String, Integer>> {
        public Tuple2<String, Integer> map(String value) {
            return new Tuple2<>(value, 1);
        }
    }

}
