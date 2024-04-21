package mc2.ingestor;

import mc2.ingestor.models.WalletTransactions.TransactionsInBlock;
import mc2.ingestor.models.WalletTransactions.UserSwap;
import mc2.ingestor.models.WalletTransactions.Transaction;

import mc2.ingestor.config.AppConfig;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.connector.pulsar.sink.PulsarSink;
import org.apache.flink.connector.pulsar.source.PulsarSource;
import org.apache.flink.connector.pulsar.source.enumerator.cursor.StartCursor;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import mc2.ingestor.utils.EnvironmentUtils;
import org.apache.flink.util.Collector;

import java.time.Duration;


public class TxsStream {
    public static void main(String[] args) throws Exception {

        // 1. Initialize the execution environment
        try (StreamExecutionEnvironment env = EnvironmentUtils.initEnvWithWebUI(false)) {

            // 2. Initialize Sources
            PulsarSource<TransactionsInBlock> txsSource =
                    EnvironmentUtils.initPulsarSource(
                            AppConfig.TXS_TOPIC,
                            "flink-wallet-tx-consumer",
                            "flink-wallet-tx-consumer",
                            StartCursor.earliest(),
                            TransactionsInBlock.class);

            WatermarkStrategy<TransactionsInBlock> watermarkStrategy =
                    WatermarkStrategy.<TransactionsInBlock>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                            .withTimestampAssigner(
                                    (SerializableTimestampAssigner<TransactionsInBlock>) (txs, _l) -> txs.getTimestamp()
                            );

            // 3. Initialize Streams
            DataStream<TransactionsInBlock> txsStream = env
                    .fromSource(txsSource, watermarkStrategy, "Pulsar Txs Source")
                    .name("pulsarTxsSource")
                    .uid("pulsarTxsSource");


            DataStream<UserSwap> swapsStream = txsStream
                    .flatMap(new FlatMapFunction<TransactionsInBlock, UserSwap>() {
                        @Override
                        public void flatMap(TransactionsInBlock transactionsInBlock, Collector<UserSwap> collector) throws Exception {
                            String userWallet = transactionsInBlock.getWallet().toLowerCase();

                            if (transactionsInBlock.getDataCount() > 1) {

                                if (transactionsInBlock.getDataCount() > 3) {
                                    System.out.println(">>>>> " + userWallet + " has more than 3 transactions in the block. Bad logic...");
                                    throw new Exception("Bad logic");
                                }

                                Transaction giveTx = null;
                                Transaction takeTx = null;

                                for (Transaction tx : transactionsInBlock.getDataList()) {
                                    if (tx.getAmount() == 0) {
                                        // This is a contract call, ignore
                                        continue;
                                    }

                                    if (tx.getFrom().toLowerCase().equals(userWallet)) {
                                        giveTx = tx;
                                    }

                                    if (tx.getTo().toLowerCase().equals(userWallet)) {
                                        takeTx = tx;
                                    }
                                }

                                if (giveTx != null && takeTx != null) {
                                    UserSwap userSwap = UserSwap.newBuilder()
                                            .setWallet(userWallet)
                                            .setFromAmount(giveTx.getAmount())
                                            .setToAmount(takeTx.getAmount())
                                            .setFromAsset(giveTx.getAsset().getSymbol())
                                            .setToAsset(takeTx.getAsset().getSymbol())
                                            .setTimestamp(giveTx.getTimestamp())
                                            .setBlockchain(giveTx.getBlockchain())
                                            .build();
                                    collector.collect(userSwap);
                                }
                            }
                        }
                    })
                    .name("swapsStream")
                    .uid("swapsStream");

            // 4. Initialize UserSwap Sink
            PulsarSink<UserSwap> userSwapSink = EnvironmentUtils.initPulsarSink(
                    AppConfig.SWAP_TOPIC,
                    "flink-wallet-swap-producer",
                    UserSwap.class);

            txsStream
                    .print()
                    .uid("Txs Stream")
                    .name("printing txs stream");

            swapsStream.sinkTo(userSwapSink)
                    .name("User Swap Sink")
                    .uid("User Swap Sink");

            env.execute("Txs Enrichment Stream");
        }
    }
}