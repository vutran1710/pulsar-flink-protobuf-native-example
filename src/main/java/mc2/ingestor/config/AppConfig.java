package mc2.ingestor.config;

import java.util.Optional;

public class AppConfig {
    public static final String SERVICE_HTTP_URL = "http://localhost:8080";
    public static final String SERVICE_URL      = "pulsar://localhost:6650";

    public static final String TXS_TOPIC = "pulsar/mobula/txs";
    public static final String SWAP_TOPIC = "pulsar/mobula/swaps";

}