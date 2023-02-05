package dataset.join;

import dataset.join.util.FinalizeJoinTransform;
import dataset.join.util.Tokenizer;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;

import java.io.FileInputStream;
import java.io.Reader;
import java.util.Properties;

public class InnerJoin {
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

        Properties properties = new Properties();
        properties.load(new FileInputStream(RESOURCE));
        String dir = properties.getProperty("dir");

        // Read people data and generate tuples out of each string read
        DataSet<Tuple2<Integer, String>> personSet = env.readTextFile(dir + INPUT1)
            .map(new Tokenizer());

        // Read location data and generate tuples out of each string read
        DataSet<Tuple2<Integer, String>> locationSet = env.readTextFile(INPUT2)
            .map(new Tokenizer());

        // Join datasets on person_id
        DataSet<Tuple3<Integer, String, String>> joined = personSet.join(locationSet)
            .where(0)
            .equalTo(0)
            .with(new FinalizeJoinTransform());

        joined.writeAsCsv(OUTPUT, "\n", " ");

        env.execute("Example: Inner Join");
    }
}
