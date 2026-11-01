import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.core.fs.Path;
import org.apache.flink.metrics.Counter;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.OnCheckpointRollingPolicy;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

public class ClickJob {

    // Plain static methods so we can unit test them without Flink
    public static String parse(String json) {
        JsonObject obj = JsonParser.parseString(json).getAsJsonObject();
        return obj.get("page").getAsString();
    }

    public static Tuple2<String, Integer> merge(Tuple2<String, Integer> a, Tuple2<String, Integer> b) {
        return Tuple2.of(a.f0, a.f1 + b.f1);
    }

    // Counts every click that passes through, for Prometheus
    public static class CountingMap extends RichMapFunction<String, Tuple2<String, Integer>> {
        private transient Counter counter;

        @Override
        public void open(Configuration parameters) {
            counter = getRuntimeContext().getMetricGroup().counter("clicks_processed");
        }

        @Override
        public Tuple2<String, Integer> map(String json) {
            counter.inc();
            return Tuple2.of(parse(json), 1);
        }
    }

    public static void main(String[] args) throws Exception {
        String outputPath = args.length > 0 ? args[0] : "file:///tmp/clicks-out";

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(10000);

        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers("kafka:29092")
                .setTopics("clicks")
                .setGroupId("click-job")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        DataStream<String> raw = env.fromSource(source, WatermarkStrategy.noWatermarks(), "kafka-clicks");

        DataStream<String> counts = raw
                .map(new CountingMap())
                .keyBy(t -> t.f0)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(10)))
                .reduce(ClickJob::merge)
                .map(t -> t.f0 + "," + t.f1 + "," + System.currentTimeMillis());

        FileSink<String> sink = FileSink
                .forRowFormat(new Path(outputPath), new SimpleStringEncoder<String>("UTF-8"))
                .withRollingPolicy(OnCheckpointRollingPolicy.build())
                .build();

        counts.sinkTo(sink);
        env.execute("click-counts");
    }
}