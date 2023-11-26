package com.example.kafkastreams.config;

import com.example.kafka.dto.Account;
import com.example.kafka.dto.Transaction;
import com.example.kafka.dto.TransactionEnriched;
import com.example.kafka.dto.User;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.subject.RecordNameStrategy;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.kafka.common.serialization.Serde;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.Map;

@Configuration
public class KafkaSerdeConfig {
    @Value("${spring.kafka.schema-registry}")
    private String schemaRegistryUrl;

    @Bean(name = "transactionSerde")
    public Serde<Transaction> getTransactionSerde() {
        Serde<Transaction> transactionSerde = new SpecificAvroSerde<>();
        transactionSerde.configure(
                Map.of(
                        AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl,
                        AbstractKafkaSchemaSerDeConfig.VALUE_SUBJECT_NAME_STRATEGY, RecordNameStrategy.class.getName(),
                        AbstractKafkaSchemaSerDeConfig.AUTO_REGISTER_SCHEMAS, false,
                        "avro.remove.java.properties", false
                ), false
        );
        return transactionSerde;
    }

    @Bean(name = "accountSerde")
    public Serde<Account> getAccountSerde() {
        Serde<Account> accountSerde = new SpecificAvroSerde<>();
        accountSerde.configure(
                Map.of(
                        AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl,
                        AbstractKafkaSchemaSerDeConfig.VALUE_SUBJECT_NAME_STRATEGY, RecordNameStrategy.class.getName(),
                        AbstractKafkaSchemaSerDeConfig.AUTO_REGISTER_SCHEMAS, false,
                        "avro.remove.java.properties", true
                ), false
        );
        return accountSerde;
    }

    @Bean(name = "userSerde")
    public Serde<User> getUserSerde() {
        Serde<User> userSerde = new SpecificAvroSerde<>();
        userSerde.configure(
                Map.of(
                        AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl,
                        AbstractKafkaSchemaSerDeConfig.VALUE_SUBJECT_NAME_STRATEGY, RecordNameStrategy.class.getName(),
                        AbstractKafkaSchemaSerDeConfig.AUTO_REGISTER_SCHEMAS, false,
                        "avro.remove.java.properties", true
                ), false
        );
        return userSerde;
    }

    @Bean(name = "transactionEnrichedSerde")
    public Serde<TransactionEnriched> getTransactionEnrichedSerde() {
        Serde<TransactionEnriched> transactionEnrichedSerde = new SpecificAvroSerde<>();
        transactionEnrichedSerde.configure(
                Map.of(
                        AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl,
                        AbstractKafkaSchemaSerDeConfig.VALUE_SUBJECT_NAME_STRATEGY, RecordNameStrategy.class.getName(),
                        AbstractKafkaSchemaSerDeConfig.AUTO_REGISTER_SCHEMAS, false,
                        "avro.remove.java.properties", false
                ), false
        );
        return transactionEnrichedSerde;
    }
}
