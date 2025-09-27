package szp.rafael.kafka.accounttransaction.stream.topology;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.Initializer;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.slf4j.Logger;
import szp.rafael.kafka.accounttransaction.model.AccountBalance;
import szp.rafael.kafka.accounttransaction.model.AccountTransaction;
import szp.rafael.kafka.accounttransaction.serde.JSONSerdes;
import szp.rafael.kafka.accounttransaction.serde.SerdeFactory;
import szp.rafael.kafka.accounttransaction.stream.join.ProcessedTransaction;

import java.math.BigDecimal;

/**
 * Iniciativa frustrada, pois ao final do processamento, embora faça o processamento correto dos saldos,
 * o agregator agrupa todas as transações em um único retorno, daí o tópico processed-transactions-topic
 * fica somente com 1 registro
 */

public class SingleResultProcessTransactionTopicTopology {

    static Logger logger = org.slf4j.LoggerFactory.getLogger(SingleResultProcessTransactionTopicTopology.class);

    public static final String ACCOUNT_TRANSACTION_TOPIC = "account-transactions";
    public static final String ACCOUNT_BALANCE_TOPIC = "account-balance";
    public static final String ACCOUNT_BALANCE_STORE = "account-balance-store";
    public static final String PROCESSED_ACCOUNT_TRANSACTIONS_TOPIC = "processed-account-transactions";


    public static Topology build() {
        StreamsBuilder builder = new StreamsBuilder();

        // Passo 1 - Capturando as transações
        KStream<String, AccountTransaction> transactionsByAccountStream = builder.stream(ACCOUNT_TRANSACTION_TOPIC, Consumed.with(Serdes.String(),getAccountTransactionSerde())).selectKey((key,transaction)-> transaction.getAccountId());

        transactionsByAccountStream.peek((key,transaction)->{
            logger.info("Conta: {}, Transação Recebida: {}",key,transaction.toJSONString());
        });

        Initializer<ProcessedTransaction> initializer = ()-> {
            logger.info("OLHA O INITIALIZER");
            return new ProcessedTransaction(null, new AccountBalance(null, BigDecimal.ZERO, 0L, 0L));
        };

        KTable<String,ProcessedTransaction> accountBalanceTable = transactionsByAccountStream
                .groupByKey(Grouped.with(Serdes.String(),getAccountTransactionSerde()))
                .aggregate(
                        initializer, //estado inicial vazio
                        (accountId,transaction,processedTransactionAggregate)->ProcessedTransaction.apply(transaction,processedTransactionAggregate.getBalance())
                        ,Materialized.with(Serdes.String(),getProcessedTransactionJSONSerdes())
                );


        //Passo 3 - Processando as transações e atualizando os saldos
        KStream<String, ProcessedTransaction> processingStream = accountBalanceTable.toStream();
        processingStream.peek((k,v)->{
            logger.info("Conta: {}, ProcessedTransaction: {}",k,v.toJSONString());
        });

        //Passo 4 Apos o processamento, o resultado de cada transacao deve ser jogado em uma fila separada
        //pois no kafka cada mensagem é imutável, inclusive em topicos compactados. De modo que é importante
        //gravar as transacoes em um topico resultante para que possamos reprocessar, se necessario.
        //O topico de saldo nao pode ser reprocessado, pois isso gravamos o id da ultima transacao processada que é um ULID
        // assim ele nunca ira reprocessar novamente a nao ser que geremos um ULID menor que o ultimo processado.
        KStream<String,AccountBalance> newAccountBalanceStream = processingStream.mapValues(ProcessedTransaction::getBalance);
        KStream<String,AccountTransaction> processedAccountTransactionsStream = processingStream.mapValues(ProcessedTransaction::getTransaction);

        logger.info("============================================================================================");
        processingStream.peek((key, transaction) -> {

            logger.info("Conta: {}, Transação Processada: {}", key, transaction.toJSONString());
        });
        newAccountBalanceStream.peek((key, balance) -> {
            logger.info("Conta: {}, Novo Saldo: {}", key, balance.toJSONString());
        });

        processedAccountTransactionsStream.to(PROCESSED_ACCOUNT_TRANSACTIONS_TOPIC, Produced.with(Serdes.String(),getAccountTransactionSerde()));//vai ficar somente com o resultado final da agregação. isso noa é ideal
        newAccountBalanceStream.to(ACCOUNT_BALANCE_TOPIC,Produced.with(Serdes.String(),getAccountBalanceSerde()));


        return builder.build();
    }


    private static JSONSerdes<AccountTransaction> getAccountTransactionSerde(){
        return  new SerdeFactory<AccountTransaction>().createSerde(AccountTransaction.class);
    }
    private static JSONSerdes<AccountBalance> getAccountBalanceSerde(){
        return  new SerdeFactory<AccountBalance>().createSerde(AccountBalance.class);
    }
    private static JSONSerdes<ProcessedTransaction> getProcessedTransactionJSONSerdes(){
        return  new SerdeFactory<ProcessedTransaction>().createSerde(ProcessedTransaction.class);
    }



}
