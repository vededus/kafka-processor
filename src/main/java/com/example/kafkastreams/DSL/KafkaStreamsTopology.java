package com.example.kafkastreams.DSL;

import com.example.kafka.dto.Account;
import com.example.kafka.dto.Transaction;
import com.example.kafka.dto.TransactionEnriched;
import com.example.kafka.dto.User;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class KafkaStreamsTopology {
    private final Serde<Transaction> transactionSerde;

    private final Serde<Account> accountSerde;

    private final Serde<User> userSerde;

    private final Serde<TransactionEnriched> transactionEnrichedSerde;

    @Autowired
    public KafkaStreamsTopology(@Qualifier("transactionSerde") Serde<Transaction> transactionSerde,
                                @Qualifier("accountSerde") Serde<Account> accountSerde,
                                @Qualifier("userSerde") Serde<User> userSerde,
                                @Qualifier("transactionEnrichedSerde") Serde<TransactionEnriched> transactionEnrichedSerde) {
        this.transactionSerde = transactionSerde;
        this.accountSerde = accountSerde;
        this.userSerde = userSerde;
        this.transactionEnrichedSerde = transactionEnrichedSerde;
    }

    @Autowired
    void buildPipeline(StreamsBuilder builder) {
        KStream<Long, Transaction> transactionKStream = builder
                .stream("transaction", Consumed.with(Serdes.Long(), transactionSerde));

        GlobalKTable<String, Account> accountGlobalKTable = builder
                .globalTable("account",
                        Materialized.with(
                                Serdes.String(),
                                accountSerde));

        transactionKStream
                .peek((key, transaction) -> log.info("Consumed transaction {} ", transaction))
                .map(((key, transaction) -> new KeyValue<>(key, createTransactionEnriched(transaction))))
                .join(accountGlobalKTable,
                        (key, transactionEnriched) -> transactionEnriched.getFromAccount(),
                        (key, transactionEnriched, account) -> updateTransactionEnrichedFromFromAccount(transactionEnriched, account))
                .peek((key, transactionEnriched) -> log.info("Producing enriched transaction to enrichedTopic: " + transactionEnriched))
                .to("enrichedTopic", Produced.with(Serdes.Long(), transactionEnrichedSerde));



        builder.stream("enrichedTopic", Consumed.with(Serdes.Long(), transactionEnrichedSerde))
                .peek((key, transactionEnriched) -> log.info("Consumed enriched transaction '{}'.", key))
                .filter((key, transactionEnriched) -> transactionEnriched.getAmount() > 500)
                .peek((key, transactionEnriched) -> log.info("Transaction '{}' passed minimal amount.", key))
                .peek((key, transactionEnriched) -> log.info("Producing filtered transaction to filteredTopic"))
                .to("filteredTopic", Produced.with(Serdes.Long(), transactionEnrichedSerde));
    }

    private TransactionEnriched createTransactionEnriched(Transaction transaction) {
        TransactionEnriched transactionEnriched = new TransactionEnriched();
        transactionEnriched.setAmount(transaction.getAmount());
        transactionEnriched.setFromAccount(transaction.getFrom());
        transactionEnriched.setToAccount(transaction.getTo());

        transactionEnriched.setFromAccountType("");
        transactionEnriched.setFromUserId("");

        transactionEnriched.setToAccountType("");
        transactionEnriched.setToUserId("");

        transactionEnriched.setFromUserName("");
        transactionEnriched.setFromUserSurname("");

        transactionEnriched.setToUserName("");
        transactionEnriched.setToUserSurname("");

        return transactionEnriched;
    }

    private TransactionEnriched updateTransactionEnrichedFromFromAccount(TransactionEnriched transactionEnriched, Account account) {
        transactionEnriched.setFromAccountType(account.getAccountType());
        transactionEnriched.setFromUserId(account.getUserId());

        return transactionEnriched;
    }
}
