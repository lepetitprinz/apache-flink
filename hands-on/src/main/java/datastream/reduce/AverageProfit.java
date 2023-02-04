package main.java.datastream.reduce;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class AverageProfit {
    public static void main(String[] args) throws Exception {
        // Set up the streaming execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<String> data = env.readTextFile("data/reduce/avg");

        // Columns: month, product, category, profit, count
        DataStream<Tuple5<String, String, String, Integer, Integer>> mapped =
            data.map(new Splitter());

        // Group by 'month'
        DataStream<Tuple5<String, String, String, Integer, Integer>> reduced =
            mapped.keyBy(t -> t.f0).reduce(new Reduce1());

        // Monthly average profit
        DataStream<Tuple2<String, Double>> profitPerMonth =
            reduced.map(new Divider());

        profitPerMonth.writeAsText("data/output/datastream/profit_per_month.txt");

        // execute program
        env.execute("Avg profit per month");
    }

    private static class Splitter
        implements MapFunction<String, Tuple5<String, String, String, Integer, Integer>> {
        public Tuple5<String, String, String, Integer, Integer> map(String value) {
            String[] words = value.split(",");
            return new Tuple5<>(words[1], words[2], words[3], Integer.parseInt(words[4]), 1);
        }
    }

    private static class Reduce1
        implements ReduceFunction<Tuple5<String, String, String, Integer, Integer>> {
        public Tuple5<String, String, String, Integer, Integer> reduce(
            Tuple5<String, String, String, Integer, Integer> current,
            Tuple5<String, String, String, Integer, Integer> pre_result) {
            return new Tuple5<>(
                current.f0, current.f1, current.f2, current.f3 + pre_result.f3, current.f4 + pre_result.f4);
        }
    }

    private static class Divider
        implements MapFunction<Tuple5<String, String, String, Integer, Integer>, Tuple2<String, Double>> {

        public Tuple2<String, Double> map(Tuple5<String, String, String, Integer, Integer> input) {
            return new Tuple2<> (input.f0, input.f3 * 1.0 / input.f4);
        }
    }
}
