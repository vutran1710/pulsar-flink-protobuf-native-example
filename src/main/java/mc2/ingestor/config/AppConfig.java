package mc2.ingestor.config;

import org.apache.flink.api.java.utils.ParameterTool;

public class AppConfig {
    public String PulsarServiceUrl;
    public String SourceTopic;
    public String SinkTopic;

    // init constructor
    public AppConfig(ParameterTool parameters) {
        this.PulsarServiceUrl = parameters.get("pulsar", "pulsar://localhost:6650");
        this.SourceTopic = parameters.get("sourceTopic", "pulsar/mobula/txs");
        this.SinkTopic = parameters.get("sinkTopic", "pulsar/mobula/txs");
    }
}