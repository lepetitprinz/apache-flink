package main.java.dataset;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.util.Collector;

public class WordCountFlatMap {

    public static void main(String[] args) {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        ParameterTool params = ParameterTool.fromArgs(args);

        env.getConfig().setGlobalJobParameters(params);

        String inputPath = "data/wordCount/wcflat.txt";
        DataSet<String> text = env.readTextFile(inputPath);
        DataSet<String> filtered = text.filter(new WordCountFilter.MyFilter());
        DataSet<Tuple2<String, Integer>> tokenized = filtered.flatMap(new FlatMapTokenizer());

    }

    public static final class FlatMapFilter implements FilterFunction<String> {
        public boolean filter(String value) throws Exception {
            return value.startsWith("N");
        }
    }

    public static final class FlatMapTokenizer implements FlatMapFunction<String, Tuple2<String, Integer>> {
        @Override
        public void flatMap(String value, Collector<Tuple2<String, Integer>> out) {
            // split the line
            String[] tokens = value.split(" ");
            for (String token : tokens) {
                if (token.length() > 0) {
                    out.collect(new Tuple2<String, Integer>(token, 1));
                }
            }
        }
    }
}
