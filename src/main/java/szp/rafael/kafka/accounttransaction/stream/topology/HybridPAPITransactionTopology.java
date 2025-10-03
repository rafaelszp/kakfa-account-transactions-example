package szp.rafael.kafka.accounttransaction.stream.topology;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.slf4j.Logger;
import szp.rafael.kafka.accounttransaction.model.AccountBalance;
import szp.rafael.kafka.accounttransaction.model.AccountTransaction;
import szp.rafael.kafka.accounttransaction.serde.JSONSerdes;
import szp.rafael.kafka.accounttransaction.serde.SerdeFactory;
import szp.rafael.kafka.accounttransaction.stream.join.ProcessedTransaction;
import szp.rafael.kafka.accounttransaction.stream.processor.TransactionProcessor;

import java.util.HashMap;
import java.util.Map;

public class HybridPAPITransactionTopology {

    static Logger logger = org.slf4j.LoggerFactory.getLogger(HybridPAPITransactionTopology.class);

    public static final String TRANSACTIONS_TOPIC = "account-transactions";
    public static final String ACCOUNT_TRANSACTIONS_TOPIC = "account-transactions-by-account";
    public static final String ACCOUNT_BALANCE_TOPIC = "account-balance";
    public static final String ACCOUNT_BALANCE_STORE = "account-balance-store";
    public static final String PROCESSED_ACCOUNT_TRANSACTIONS_STORE = "processed-account-transactions-store";
    public static final String PROCESSED_ACCOUNT_TRANSACTIONS_TOPIC = "processed-account-transactions";


    public static Topology build() {
        StreamsBuilder builder = new StreamsBuilder();

        // Passo 1 - Capturando as transações

        KStream<String, AccountTransaction> transactionsStream = builder.stream(TRANSACTIONS_TOPIC, Consumed.with(Serdes.String(),getAccountTransactionSerde()));
        transactionsStream
                .selectKey((key, transaction)-> transaction.getAccountId())
                .to(ACCOUNT_TRANSACTIONS_TOPIC,Produced.with(Serdes.String(),getAccountTransactionSerde()));


        //Passo 2 - Como o chaveamento foi feito pela transaction ID, preciso fazer um rekey para um topico correto,
        // para que as transações de cada conta fiquem na mesma partição
        KStream<String, AccountTransaction> transactionsByAccountStream = builder.stream(ACCOUNT_TRANSACTIONS_TOPIC, Consumed.with(Serdes.String(),getAccountTransactionSerde()));

        transactionsByAccountStream.peek((key,transaction)->{
            logger.info("\n\n\n\n\n");
            logger.info("###############################################################################################################");
            logger.info("Conta: {}, Transação Recebida: {}",key,transaction.toJSONString());
        });

        Map<String, String> changelogConfig = new HashMap<>();
        changelogConfig.put("min.insync.replicas", "1");


        // state store for digital twin records
        StoreBuilder<KeyValueStore<String, AccountBalance>> balanceStoreBuilder
                =
                Stores.keyValueStoreBuilder(
                        Stores.persistentKeyValueStore(ACCOUNT_BALANCE_STORE),
                        Serdes.String(),
                        getAccountBalanceSerde())
                        .withLoggingEnabled(changelogConfig);

        StoreBuilder<KeyValueStore<String, ProcessedTransaction>> processedTransactionsStoreBuilder =
                Stores.keyValueStoreBuilder(
                        Stores.persistentKeyValueStore(PROCESSED_ACCOUNT_TRANSACTIONS_STORE),
                        Serdes.String(),
                        getProcessedTransactionJSONSerdes())
                        .withLoggingEnabled(changelogConfig);


        builder.addStateStore(balanceStoreBuilder);
        builder.addStateStore(processedTransactionsStoreBuilder);

        KStream<String,ProcessedTransaction> processedTransactionStream = transactionsByAccountStream
                .process(new TransactionProcessor(ACCOUNT_BALANCE_STORE),ACCOUNT_BALANCE_STORE,PROCESSED_ACCOUNT_TRANSACTIONS_STORE);


        //Passo 3 - Processando as transações e atualizando os saldos

        processedTransactionStream.peek((k,v)->{
            logger.info("Conta: {}, ProcessedTransaction: {}",k,v.toJSONString());
        });

        //Passo 4 Apos o processamento, o resultado de cada transacao deve ser jogado em uma fila separada
        //pois no kafka cada mensagem é imutável, inclusive em topicos compactados. De modo que é importante
        //gravar as transacoes em um topico resultante para que possamos reprocessar, se necessario.
        //O topico de saldo nao pode ser reprocessado, pois isso gravamos o id da ultima transacao processada que é um ULID
        // assim ele nunca ira reprocessar novamente a nao ser que geremos um ULID menor que o ultimo processado.
        KStream<String,AccountBalance> newAccountBalanceStream = processedTransactionStream.mapValues(ProcessedTransaction::getBalance);


        processedTransactionStream.peek((key, transaction) -> {

            logger.info("Conta: {}, Transação Processada: {}", key, transaction.toJSONString());
        });
        newAccountBalanceStream.peek((key, balance) -> {
            logger.info("Conta: {}, Novo Saldo: {}", key, balance.toJSONString());
        });

        processedTransactionStream.to(PROCESSED_ACCOUNT_TRANSACTIONS_TOPIC,Produced.with(Serdes.String(),getProcessedTransactionJSONSerdes()));
        newAccountBalanceStream.to(ACCOUNT_BALANCE_TOPIC,Produced.with(Serdes.String(),getAccountBalanceSerde()));


        return builder.build();
    }


    public static JSONSerdes<AccountTransaction> getAccountTransactionSerde(){
        return  new SerdeFactory<AccountTransaction>().createSerde(AccountTransaction.class);
    }
    public static JSONSerdes<AccountBalance> getAccountBalanceSerde(){
        return  new SerdeFactory<AccountBalance>().createSerde(AccountBalance.class);
    }
    public static JSONSerdes<ProcessedTransaction> getProcessedTransactionJSONSerdes(){
        return  new SerdeFactory<ProcessedTransaction>().createSerde(ProcessedTransaction.class);
    }



}
