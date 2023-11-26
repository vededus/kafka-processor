package com.example.kafkastreams.DSL;

import com.example.kafka.dto.Account;
import com.example.kafka.dto.Transaction;
import com.example.kafka.dto.TransactionEnriched;
import com.example.kafka.dto.User;
import com.example.kafkastreams.PROCESSOR.processors.EnrichmentProcessor;
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
                .stream("transaction", /* <--input topic name */
                        Consumed.with(
                                Serdes.Long(),/* <--key serde */
                                transactionSerde/* <--value serde */
                        ));
        GlobalKTable<String, Account> accountGlobalKTable = builder.globalTable("account",
                Materialized.with(Serdes.String(), accountSerde));

        GlobalKTable<String, User> userGlobalKTable = builder.globalTable("user",
                Materialized.with(Serdes.String(),userSerde ));


        builder.stream("transaction", Consumed.with(Serdes.Long(), transactionSerde))
                .peek((key, transaction) -> log.info("Consumed transaction: " + transaction))
                .map(((key, transaction) -> new KeyValue<>(key, createTransactionEnriched(transaction))))
                .join(accountGlobalKTable,
                        (key, transactionEnriched) -> transactionEnriched.getFromAccount(),
                        (key, transactionEnriched, account) -> updateTransactionEnrichedFromFromAccount(transactionEnriched, account))
                .join(userGlobalKTable,
                        (key, transactionEnriched) -> transactionEnriched.getFromUserId(),
                        this::updateEnrichedTransactionWithFromUser)
                .join(accountGlobalKTable,
                        (key, transactionEnriched) -> transactionEnriched.getToAccount(),
                        this::updateTransactionEnrichedWithToAccount)
                .join(userGlobalKTable,
                        (key, transactionEnriched) -> transactionEnriched.getToUserId(),
                        this::updateEnrichedTransactionWithToUser)
                .peek((key, transactionEnriched) -> log.info("Producing enriched transaction to enrichedTopic: " + transactionEnriched))
                .to("enrichedTopic", Produced.with(Serdes.Long(), transactionEnrichedSerde));

        builder.stream("enrichedTopic", Consumed.with(Serdes.Long(), transactionEnrichedSerde))
                .peek((key, transactionEnriched) -> log.info("Consumed transaction '{}'.", key))
                .filter((key, transactionEnriched) -> !transactionEnriched.getFromUserId().equals(transactionEnriched.getToUserId()))
                .peek((key, transactionEnriched) -> log.info("Transaction '{}' passed same user filter.", key))
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
        return transactionEnriched;
    }

    private TransactionEnriched updateTransactionEnrichedFromFromAccount(TransactionEnriched transactionEnriched, Account account) {
        transactionEnriched.setFromAccountType(account.getAccountType());
        transactionEnriched.setFromUserId(account.getUserId());
        return transactionEnriched;
    }

    private TransactionEnriched updateTransactionEnrichedWithToAccount(TransactionEnriched transactionEnriched, Account account) {
        transactionEnriched.setToAccountType(account.getAccountType());
        transactionEnriched.setToUserId(account.getUserId());
        return transactionEnriched;
    }

    private TransactionEnriched updateEnrichedTransactionWithToUser(TransactionEnriched transactionEnriched, User user) {
        transactionEnriched.setToUserName(user.getName());
        transactionEnriched.setToUserSurname(user.getSurname());
        return transactionEnriched;
    }

    private TransactionEnriched updateEnrichedTransactionWithFromUser(TransactionEnriched transactionEnriched, User user) {
        transactionEnriched.setFromUserName(user.getName());
        transactionEnriched.setFromUserSurname(user.getSurname());
        return transactionEnriched;
    }
}
