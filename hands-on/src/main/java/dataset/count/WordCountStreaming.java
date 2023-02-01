package dataset.count;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class WordCountStreaming {

    public static void main(String[] args) {
        // set up the execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Check input parameters
        final ParameterTool params = ParameterTool.fromArgs(args);

        // Make parameters available in the web interface
        env.getConfig().setGlobalJobParameters(params);

    }
}
