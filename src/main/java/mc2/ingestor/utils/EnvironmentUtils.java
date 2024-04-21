package mc2.ingestor.utils;

import static org.apache.flink.configuration.TaskManagerOptions.CPU_CORES;
import static org.apache.flink.configuration.TaskManagerOptions.MANAGED_MEMORY_SIZE;
import static org.apache.flink.configuration.TaskManagerOptions.TASK_HEAP_MEMORY;
import static org.apache.flink.configuration.TaskManagerOptions.TASK_OFF_HEAP_MEMORY;
import mc2.ingestor.config.AppConfig;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.connector.pulsar.sink.PulsarSink;
import org.apache.flink.connector.pulsar.source.PulsarSource;
import org.apache.flink.connector.pulsar.source.enumerator.cursor.StartCursor;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.pulsar.client.api.Schema;


public class EnvironmentUtils {
    public static StreamExecutionEnvironment initEnvWithWebUI(boolean withWebUI){
        Configuration flinkConfig = new Configuration();

        flinkConfig.set(CPU_CORES, 4.0);
        flinkConfig.set(TASK_HEAP_MEMORY, MemorySize.ofMebiBytes(1024));
        flinkConfig.set(TASK_OFF_HEAP_MEMORY, MemorySize.ofMebiBytes(256));
        flinkConfig.set(MANAGED_MEMORY_SIZE, MemorySize.ofMebiBytes(1024));

        if (withWebUI) {
            return StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(flinkConfig);
        } else {
            return StreamExecutionEnvironment.getExecutionEnvironment();
        }
    }

    public static <T extends com.google.protobuf.GeneratedMessageV3> PulsarSource<T> initPulsarSource(String topicName,
                                                       String subscriptionName,
                                                       String consumerName,
                                                       StartCursor startCursor,
                                                       Class<T> classz) {

        Schema<T> schema = Schema.PROTOBUF_NATIVE(classz);

        return PulsarSource.builder()
                .setServiceUrl(AppConfig.SERVICE_URL)
                .setStartCursor(startCursor)
                .setTopics(topicName)
                .setDeserializationSchema(schema, classz)
                .setSubscriptionName(subscriptionName)
                .setConsumerName(consumerName)
                .build();
    }

    public static <T extends com.google.protobuf.GeneratedMessageV3> PulsarSink<T> initPulsarSink(String topicName,
                                                                                                    String producerName,
                                                                                                    Class<T> classz) {

        Schema<T> schema = Schema.PROTOBUF_NATIVE(classz);

        return PulsarSink.builder()
                .setServiceUrl(AppConfig.SERVICE_URL)
                .setProducerName(producerName)
                .setTopics(topicName)
                .setSerializationSchema(schema, classz)
                .build();
    }

}