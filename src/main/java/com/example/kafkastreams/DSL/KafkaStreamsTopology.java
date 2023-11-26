package com.example.kafkastreams.DSL;

import com.example.kafka.dto.Account;
import com.example.kafka.dto.Transaction;
import com.example.kafka.dto.TransactionEnriched;
import com.example.kafka.dto.User;
import com.example.kafkastreams.PROCESSOR.processors.EnrichmentProcessor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;
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
        builder.globalTable("account",
                Materialized.<String, Account, KeyValueStore<Bytes, byte[]>>as("account-state-store")
                        .withKeySerde(Serdes.String())
                        .withValueSerde(accountSerde));
        builder.globalTable("user",
                Materialized.<String, User, KeyValueStore<Bytes, byte[]>>as("user-state-store")
                        .withKeySerde(Serdes.String())
                        .withValueSerde(userSerde));

        builder.stream("transaction", Consumed.with(Serdes.Long(), transactionSerde))
                .process(() -> new EnrichmentProcessor())
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
}
