package datastream;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

public class SplitEx {
    private static final String DIR = System.getProperty("user.dir");
    private static final String INPUT = DIR + "/data/input/datastream/oddeven";
    private static final String OUTPUT = DIR + "/data/output/datastream";

    // tag side output for even values
    final static OutputTag<String> evenOutTag = new OutputTag<String>("even-string-output") {};
    final static OutputTag<Integer> oddOutTag = new OutputTag<Integer>("odd-int-output") {};
    public static void main(String[] args) throws Exception {
        // Check input parameters
        final ParameterTool params = ParameterTool.fromArgs(args);

        // Set up the stream execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Make parameters available in the web interface
        env.getConfig().setGlobalJobParameters(params);

        DataStream<String> text = env.readTextFile(INPUT);

        SingleOutputStreamOperator<Integer> mainStream = text.process(new SplitProcess());

        DataStream<String> evenSideOutputStream = mainStream.getSideOutput(evenOutTag);
        DataStream<Integer> oddSideOutputStream = mainStream.getSideOutput(oddOutTag);

        evenSideOutputStream.writeAsText(OUTPUT + "/even.txt", FileSystem.WriteMode.OVERWRITE);
        oddSideOutputStream.writeAsText(OUTPUT + "/odd.txt", FileSystem.WriteMode.OVERWRITE);

        // Execute program
        env.execute("Odd Even");
    }

    private static class SplitProcess extends ProcessFunction<String, Integer> {
        @Override
        public void processElement(String value, Context context, Collector<Integer> out) throws Exception {
            int intVal = Integer.parseInt(value);

            // get all data in regular output as well
            out.collect(intVal);

            if (intVal % 2 == 0) {
                // emit data to side output for even output
                context.output(evenOutTag, String.valueOf(intVal));
            } else {
                // emit data to side output for odd output
                context.output(oddOutTag, intVal);
            }
        }
    }
}
