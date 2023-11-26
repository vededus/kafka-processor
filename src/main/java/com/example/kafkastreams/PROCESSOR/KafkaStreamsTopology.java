package com.example.kafkastreams.PROCESSOR;

import com.example.kafka.dto.Account;
import com.example.kafka.dto.Transaction;
import com.example.kafka.dto.TransactionEnriched;
import com.example.kafka.dto.User;
import com.example.kafkastreams.PROCESSOR.processors.EnrichmentProcessor;
import com.example.kafkastreams.PROCESSOR.processors.FilterProcessor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Materialized;
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
        builder.globalTable("account", /* <-- input topic name */
                Materialized.<String, Account, KeyValueStore<Bytes, byte[]>>as("account-state-store")/* <-- state store name */
                        .withKeySerde(Serdes.String()) /* <-- key serde */
                        .withValueSerde(accountSerde));/* <-- value serde */

        builder.globalTable("user",
                Materialized.<String, User, KeyValueStore<Bytes, byte[]>>as("user-state-store")
                        .withKeySerde(Serdes.String())
                        .withValueSerde(userSerde));

        Topology topology = builder.build();

        topology.addSource(
                        "TransactionSource",
                        Serdes.Long().deserializer(),
                        transactionSerde.deserializer(),
                        "transaction")
                .addProcessor(
                        "EnrichmentProcessor",
                        EnrichmentProcessor::new,
                        "TransactionSource")
                .addSink(
                        "EnrichmentSink",
                        "enrichedTopic",
                        Serdes.Long().serializer(),
                        transactionEnrichedSerde.serializer(),
                        "EnrichmentProcessor")
                .addSource(
                        "EnrichmentSource",
                        Serdes.Long().deserializer(),
                        transactionEnrichedSerde.deserializer(),
                        "enrichedTopic")
                .addProcessor(
                        "FilterProcessor",
                        FilterProcessor::new,
                        "EnrichmentSource")
                .addSink(
                        "FilterSink",
                        "filteredTopic",
                        Serdes.Long().serializer(),
                        transactionSerde.serializer(),
                        "FilterProcessor");
    }
}
