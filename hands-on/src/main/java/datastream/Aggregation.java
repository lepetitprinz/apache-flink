package datastream;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Aggregation {
    private static final String DIR = System.getProperty("user.dir");
    private static final String INPUT = DIR + "/data/input/datastream/avg1";
    private static final String OUTPUT = DIR + "/data/output/datastream/";
    public static void main(String[] args) throws Exception {
        // set up the streaming execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<String> data = env.readTextFile(INPUT);
        DataStream<Tuple4<String, String, String, Integer>> mapped = data.map(new Splitter());

        // Aggregate data
        mapped.keyBy(t -> t.f0).sum(3).writeAsText(
            OUTPUT + "agg_sum.txt", FileSystem.WriteMode.OVERWRITE);
        mapped.keyBy(t -> t.f0).min(3).writeAsText(
            OUTPUT + "agg_min.txt", FileSystem.WriteMode.OVERWRITE);
        mapped.keyBy(t -> t.f0).minBy(3).writeAsText(
            OUTPUT + "agg_minby.txt", FileSystem.WriteMode.OVERWRITE);
        mapped.keyBy(t -> t.f0).max(3).writeAsText(
            OUTPUT + "agg_max.txt", FileSystem.WriteMode.OVERWRITE);
        mapped.keyBy(t -> t.f0).maxBy(3).writeAsText(
            OUTPUT + "agg_maxby.txt", FileSystem.WriteMode.OVERWRITE);

        // execute program
        env.execute("Aggregation");
    }

    private static class Splitter
        implements MapFunction<String, Tuple4<String, String, String, Integer>> {
        public Tuple4<String, String, String, Integer> map(String value) {
            String[] words = value.split(",");
            return new Tuple4<>(words[1], words[2], words[3], Integer.parseInt(words[4]));
        }
    }
}
