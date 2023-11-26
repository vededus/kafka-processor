package com.example.kafkastreams.exception;

public class EnrichmentException extends RuntimeException {
    private String field;

    public EnrichmentException(String field) {
        super(field);
        this.field = field;
    }

    public String getField() {
        return field;
    }
}
