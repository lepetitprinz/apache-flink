package dataset.join.util;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;

public final class FinalizeJoinTransform implements JoinFunction<Tuple2<Integer, String>, Tuple2<Integer, String>, Tuple3<Integer, String, String>> {
    @Override
    public Tuple3<Integer, String, String> join(
        Tuple2<Integer, String> person, Tuple2<Integer, String> location
    ) throws Exception {
        return new Tuple3<Integer, String, String>(person.f0, person.f1, location.f1);
    }
}
