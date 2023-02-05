package dataset.join.util;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;

public final class Tokenizer implements MapFunction<String, Tuple2<Integer, String>> {
    public Tuple2<Integer, String> map(String value) {
        String[] words = value.split(",");

        return new Tuple2<Integer, String>(Integer.parseInt(words[0]), words[1]);
    }
}