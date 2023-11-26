package com.example.kafkastreams.PROCESSOR.processors;

import com.example.kafka.dto.TransactionEnriched;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;

@Slf4j
public class FilterProcessor implements Processor<Long, TransactionEnriched, Long, TransactionEnriched> {
    private ProcessorContext<Long, TransactionEnriched> context;

    @Override
    public void init(ProcessorContext<Long, TransactionEnriched> context) {
        Processor.super.init(context);
        this.context = context;
    }

    @Override
    public void process(Record<Long, TransactionEnriched> record) {
        TransactionEnriched transactionEnriched = record.value();
        if (transactionEnriched.getAmount() > 500){
            log.info("Transaction '{}' have passed minimum amount filter", record.key());
            if(!transactionEnriched.getToUserId().equals(transactionEnriched.getFromUserId())) {
                log.info("Transaction '{}' have passed same user filter", record.key());
                context.forward(record);
            }
            else {
                log.info("Transaction '{}' did not pass same user filter", record.key());
            }
        } else {
            log.info("Transaction '{}' did not pass minimum amount filter", record.key());
        }
    }

    @Override
    public void close() {
        Processor.super.close();
    }
}
