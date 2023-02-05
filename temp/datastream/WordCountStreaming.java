package main.java.datastream;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class WordCountStreaming {

    public static void main(String[] args) throws Exception {
        // Set up the stream execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Check input parameters
        final ParameterTool params = ParameterTool.fromArgs(args);

        // Make parameters available in the web interface
        env.getConfig().setGlobalJobParameters(params);

        DataStream<String> text = env.socketTextStream("localhost", 9999);

        DataStream<Tuple2<String, Integer>> counts = text
            .filter(new StartWithFilter())
            .map(new Tokenizer())
            .keyBy(t -> t.f0)  // Logically partitions a stream into disjoint partitions
            .sum(1);

        counts.print();

        // execute program
        env.execute("Streaming Word Count");
    }

    private static final class StartWithFilter implements FilterFunction<String> {
        @Override
        public boolean filter(String value) throws Exception {
            return value.startsWith("N");
        }
    }

    private static final class Tokenizer implements MapFunction<String, Tuple2<String, Integer>> {
        public Tuple2<String, Integer> map(String value) {
            return new Tuple2<>(value, 1);
        }
    }
}
