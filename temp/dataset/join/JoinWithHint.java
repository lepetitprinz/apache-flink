package dataset.join;

import dataset.join.util.FinalizeJoinTransform;
import dataset.join.util.Tokenizer;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.api.common.operators.base.JoinOperatorBase.JoinHint;

public class JoinWithHint {
    private static final String RESOURCE = "config/config.properties";
    private static final String DIR = "/Users/yjkim-studio/src/flink/hands-on/data/";
    private static final String INPUT1 = "join/person";
    private static final String INPUT2= DIR + "join/location";
    private static final String OUTPUT = DIR + "output/innerJoin.csv";

    public static void main(String[] args) throws Exception {
        // Set up the execution environment
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        final ParameterTool params = ParameterTool.fromArgs(args);

        // make parameters available in the web interface
        env.getConfig().setGlobalJobParameters(params);

        // Read people data and generate tuples out of each string read
        DataSet<Tuple2<Integer, String>> personSet = env.readTextFile(INPUT1)
            .map(new Tokenizer());

        // Read location data and generate tuples out of each string read
        DataSet<Tuple2<Integer, String>> locationSet = env.readTextFile(INPUT2)
            .map(new Tokenizer());

        /*
         * Join datasets with hint
         * JoinHint Options:
         * - OPTIMIZER_CHOOSES: Leave the choice how to do the join to the optimizer.
         * - BROADCAST_HASH_FIRST: the first join input is much smaller than the second.
         * - BROADCAST_HASH_SECOND: the second join input is much smaller than the first
         * - REPARTITION_HASH_FIRST: the first join input is a bit smaller than the second.
         * - REPARTITION_HASH_SECOND: the second join input is a bit smaller than the first.
         * - REPARTITION_SORT_MERGE: the join should repartitioning both inputs and use sorting and merging as the join strategy.
         */

        DataSet<Tuple3<Integer, String, String>> joined = personSet
            .join(locationSet, JoinHint.BROADCAST_HASH_FIRST)
            .where(0)
            .equalTo(0)
            .with(new FinalizeJoinTransform());

        joined.writeAsCsv(OUTPUT, "\n", " ");

        env.execute("Example: Inner Join");
    }
}
