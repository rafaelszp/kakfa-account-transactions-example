package szp.rafael.kafka.accounttransaction;


import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.state.KeyValueStore;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import szp.rafael.kafka.accounttransaction.model.AccountBalance;
import szp.rafael.kafka.accounttransaction.model.AccountTransaction;
import szp.rafael.kafka.accounttransaction.serde.JSONSerdes;
import szp.rafael.kafka.accounttransaction.stream.join.ProcessedTransaction;
import szp.rafael.kafka.accounttransaction.stream.topology.HybridPAPITransactionTopology;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Properties;


public class MainAppTest {

    Properties streamProps = new Properties();

    @BeforeEach
    public void setup(){
        streamProps.put("application.id", "test-app");
        streamProps.put("auto.offset.reset", "earliest");
        streamProps.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        streamProps.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, JSONSerdes.class.getName());

    }


    @Test
    public void should_process_account_transactions(){

        try (final TopologyTestDriver testDriver = new TopologyTestDriver(HybridPAPITransactionTopology.build(), streamProps)) {
            final TestInputTopic<String, AccountTransaction> accountTransactionsTopic = testDriver.createInputTopic(
                    HybridPAPITransactionTopology.TRANSACTIONS_TOPIC,
                    Serdes.String().serializer(),
                    HybridPAPITransactionTopology.getAccountTransactionSerde().serializer()
            );
            final KeyValueStore<String, AccountBalance> store = testDriver.getKeyValueStore(HybridPAPITransactionTopology.ACCOUNT_BALANCE_STORE);

            String accountId = "ACC_01";
            final List<AccountTransaction> transactions = List.of(
                    new AccountTransaction(accountId,"TXN_01", AccountTransaction.TransactionType.CREDIT, BigDecimal.valueOf(1000),null,null),
                    new AccountTransaction(accountId,"TXN_02", AccountTransaction.TransactionType.DEBIT, BigDecimal.valueOf(500),null,null),
                    new AccountTransaction(accountId,"TXN_03", AccountTransaction.TransactionType.DEBIT, BigDecimal.valueOf(5000),null,null), // should be declined
                    new AccountTransaction(accountId,"TXN_04", AccountTransaction.TransactionType.CREDIT, BigDecimal.valueOf(1000),null,null),
                    new AccountTransaction(accountId,"TXN_05", AccountTransaction.TransactionType.DEBIT, BigDecimal.valueOf(1500),null,null)
            );

            transactions.forEach(txn-> accountTransactionsTopic.pipeInput(txn.getTransactionId(),txn,txn.getProcessedTimestamp()==null?null:txn.getProcessedTimestamp()+1));

            final BigDecimal expectedValue = BigDecimal.ZERO.setScale(2, RoundingMode.HALF_EVEN);
            testDriver.advanceWallClockTime(Duration.of(1, ChronoUnit.SECONDS));
            AccountBalance accountBalance = store.get(accountId);
            Assertions.assertEquals(expectedValue,accountBalance.getAmount());

            final TestOutputTopic<String,ProcessedTransaction> processedAccountTransactionsTopic = testDriver.createOutputTopic(
                    HybridPAPITransactionTopology.PROCESSED_ACCOUNT_TRANSACTIONS_TOPIC,
                    Serdes.String().deserializer(),
                    HybridPAPITransactionTopology.getProcessedTransactionJSONSerdes().deserializer()
            );
            final int expectedRejectedCount = 1;
            final int expectedCompletedCount = transactions.size() - expectedRejectedCount;

            List<ProcessedTransaction> processedTransactions = processedAccountTransactionsTopic.readValuesToList();
            Assertions.assertEquals(expectedCompletedCount,processedTransactions.stream().filter(pt-> AccountTransaction.TransactionStatus.COMPLETED.equals(pt.getTransaction().getStatus())).count());
            Assertions.assertEquals(expectedRejectedCount,processedTransactions.stream().filter(pt-> AccountTransaction.TransactionStatus.REJECTED.equals(pt.getTransaction().getStatus())).count());


        }



    }

}
