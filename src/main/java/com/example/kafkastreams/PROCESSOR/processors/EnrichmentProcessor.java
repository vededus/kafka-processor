package com.example.kafkastreams.PROCESSOR.processors;

import com.example.kafka.dto.Account;
import com.example.kafka.dto.Transaction;
import com.example.kafka.dto.TransactionEnriched;
import com.example.kafka.dto.User;
import com.example.kafkastreams.exception.EnrichmentException;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.processor.To;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.ValueAndTimestamp;
import org.springframework.stereotype.Component;

import java.util.Objects;

@Slf4j
public class EnrichmentProcessor implements Processor<Long, Transaction, Long, TransactionEnriched> {

    private ProcessorContext<Long, TransactionEnriched> context;

    private KeyValueStore<String, ValueAndTimestamp<Account>> accountGlobalStore;

    private KeyValueStore<String, ValueAndTimestamp<User>> userGlobalStore;

    @Override
    public void init(ProcessorContext<Long, TransactionEnriched> context) {
        Processor.super.init(context);
        this.accountGlobalStore = context.getStateStore("account-state-store");
        this.userGlobalStore = context.getStateStore("user-state-store");
        this.context = context;
    }

    @Override
    public void process(Record<Long, Transaction> record) {
        Transaction transaction = record.value();
        Long key = record.key();

        try {
            Account toAccount = getAccountFromStateStore(transaction.getTo());
            Account fromAccount = getAccountFromStateStore(transaction.getFrom());
            User toUser = getUserFromStateStore(toAccount.getUserId());
            User fromUser = getUserFromStateStore(fromAccount.getUserId());
            log.info("Producing enriched transaction");

            context.forward(new Record(key, mapAccountsAndUsersToTransactionEnriched(transaction, toAccount, toUser, fromAccount, fromUser), context.currentSystemTimeMs()));
        } catch (EnrichmentException e) {
            log.error("Enrichment error for key {}", e.getField());
        }
    }

    private Account getAccountFromStateStore(String key) {
        ValueAndTimestamp<Account> valueAndTimestamp = accountGlobalStore.get(key);
        if (Objects.isNull(valueAndTimestamp)) {
            throw new EnrichmentException(key);
        }
        return valueAndTimestamp.value();
    }

    private User getUserFromStateStore(String key) {
        ValueAndTimestamp<User> valueAndTimestamp = userGlobalStore.get(key);
        if (Objects.isNull(valueAndTimestamp)) {
            throw new EnrichmentException(key);
        }
        return valueAndTimestamp.value();
    }

    private TransactionEnriched mapAccountsAndUsersToTransactionEnriched(Transaction transaction, Account toAccount, User toUser, Account fromAccount, User fromUser) {
        TransactionEnriched transactionEnriched = new TransactionEnriched();

        transactionEnriched.setAmount(transaction.getAmount());
        transactionEnriched.setFromAccount(transaction.getFrom());
        transactionEnriched.setToAccount(transaction.getTo());
        transactionEnriched.setFromAccountType(fromAccount.getAccountType());
        transactionEnriched.setFromUserId(fromAccount.getUserId());
        transactionEnriched.setFromUserName(fromUser.getName());
        transactionEnriched.setFromUserSurname(fromUser.getSurname());
        transactionEnriched.setToAccountType(toAccount.getAccountType());
        transactionEnriched.setToUserId(toAccount.getUserId());
        transactionEnriched.setToUserName(toUser.getName());
        transactionEnriched.setToUserSurname(toUser.getSurname());

        return transactionEnriched;
    }

    @Override
    public void close() {
        Processor.super.close();
    }
}
