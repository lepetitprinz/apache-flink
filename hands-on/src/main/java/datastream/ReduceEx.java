package datastream;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class ReduceEx {
    private static final String DIR = System.getProperty("user.dir");
    private static final String INPUT = DIR + "/data/input/reduce/avg";
    private static final String OUTPUT = "/data/output/datastream/profit_per_month.txt";

    public static void main(String[] args) throws Exception {

        // Set up the streaming execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<String> data = env.readTextFile(INPUT);

        // Columns: month, product, category, profit, count
        DataStream<Tuple5<String, String, String, Integer, Integer>> mapped =
            data.map(new Splitter());

        // Group by 'month'
        DataStream<Tuple5<String, String, String, Integer, Integer>> reduced =
            mapped.keyBy(t -> t.f0).reduce(new Reduce1());

        // Monthly average profit
        DataStream<Tuple2<String, Double>> profitPerMonth =
            reduced.map(new Divider());

        profitPerMonth.writeAsText(OUTPUT);

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
            Tuple5<String, String, String, Integer, Integer> v1,
            Tuple5<String, String, String, Integer, Integer> v2) {
            return new Tuple5<>(
                v1.f0, v1.f1, v1.f2, v1.f3 + v2.f3, v1.f4 + v2.f4);
        }
    }

    private static class Divider
        implements MapFunction<Tuple5<String, String, String, Integer, Integer>, Tuple2<String, Double>> {

        public Tuple2<String, Double> map(Tuple5<String, String, String, Integer, Integer> input) {
            return new Tuple2<> (input.f0, input.f3 * 1.0 / input.f4);
        }
    }
}
