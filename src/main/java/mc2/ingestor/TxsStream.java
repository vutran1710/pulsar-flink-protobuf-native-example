package mc2.ingestor;

import mc2.ingestor.models.WalletTransactions.TransactionsInBlock;
import mc2.ingestor.models.WalletTransactions.UserSwap;
import mc2.ingestor.models.WalletTransactions.Transaction;

import mc2.ingestor.config.AppConfig;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.connector.pulsar.source.PulsarSource;
import org.apache.flink.connector.pulsar.source.enumerator.cursor.StartCursor;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import mc2.ingestor.utils.EnvironmentUtils;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;


public class TxsStream {
    public static void main(String[] args) throws Exception {

        // 1. Initialize the execution environment
        try (StreamExecutionEnvironment env = EnvironmentUtils.initEnvWithWebUI(false)) {

            // 2. Initialize Sources
            PulsarSource<TransactionsInBlock> txsSource =
                    EnvironmentUtils.initPulsarSource(
                            AppConfig.TXS_TOPIC,
                            "flink-wallet-tx-consumer",
                            StartCursor.earliest(),
                            TransactionsInBlock.class);


            WatermarkStrategy<TransactionsInBlock> watermarkStrategy =
                    WatermarkStrategy.<TransactionsInBlock>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                            .withTimestampAssigner(
                                    (SerializableTimestampAssigner<TransactionsInBlock>) (txs, _l) -> txs.getTimestamp()
                            );

            // 3. Initialize Streams
            DataStream<TransactionsInBlock> txsStream =
                    env.fromSource(txsSource, watermarkStrategy, "Pulsar Txs Source")
                            .name("pulsarTxsSource")
                            .uid("pulsarTxsSource");

            DataStream<UserSwap> userSwapDataStream = txsStream.flatMap(new FlatMapFunction<TransactionsInBlock, UserSwap>() {
                @Override
                public void flatMap(TransactionsInBlock transactionsInBlock, Collector<UserSwap> collector) throws Exception {
                    String userWallet = transactionsInBlock.getWallet();
                    if (transactionsInBlock.getDataCount() > 1) {
                        HashMap<String, Transaction> gives = new HashMap<>();
                        HashMap<String, Transaction> takes = new HashMap<>();

                        for (Transaction tx : transactionsInBlock.getDataList()) {
                            if (tx.getFrom().equals(userWallet)) {
                                gives.put(tx.getTo(), tx);
                            }

                            if (tx.getTo().equals(userWallet)) {
                                takes.put(tx.getFrom(), tx);
                            }
                        }

                        // Loop through exchangeTo and exchangeFrom to find the swaps
                        Set<String> swapKeys = new HashSet<>(gives.keySet());
                        swapKeys.retainAll(takes.keySet());
                        // Loop through the swapKeys and emit the UserSwap
                        for (String swapKey : swapKeys) {
                            Transaction giveTx = gives.get(swapKey);
                            Transaction takeTx = takes.get(swapKey);
                            UserSwap userSwap = UserSwap.newBuilder()
                                    .setFromAddr(giveTx.getFrom())
                                    .setToAddr(giveTx.getTo())
                                    .setFromAmount(giveTx.getAmount())
                                    .setToAmount(takeTx.getAmount())
                                    .setFromAsset(giveTx.getAsset().getSymbol())
                                    .setToAsset(takeTx.getAsset().getSymbol())
                                    .setTimestamp(giveTx.getTimestamp())
                                    .setFromTxHash(giveTx.getHash())
                                    .setToTxHash(takeTx.getHash())
                                    .build();
                            collector.collect(userSwap);
                        }
                    }
                }

            });

            txsStream
                    .print()
                    .uid("print")
                    .name("print");

            userSwapDataStream
                    .print()
                    .uid("Swaps --")
                    .name("WalletSwaps");

            env.execute("Txs Enrichment Stream");
        }
    }
}