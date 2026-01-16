package io.github.simple.dynamodb.processor;

import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

public class KeyPair {

    private Object hashKey;

    private AttributeValue hashKeyAttributeValue;

    private Object rangeKey;

    private AttributeValue rangeKeyAttributeValue;

    private int hashCode;

    public KeyPair() {
    }

    public KeyPair(Object hashKey) {
        this.hashKey = hashKey;
    }

    public KeyPair(Object hashKey, Object rangeKey) {
        this.hashKey = hashKey;
        this.rangeKey = rangeKey;
    }

    public void setHashKey(Object hashKey) {
        this.hashKey = hashKey;
        this.hashKeyAttributeValue = null;
    }

    public void setRangeKey(Object rangeKey) {
        this.rangeKey = rangeKey;
        this.rangeKeyAttributeValue = null;
    }

    public Object getHashKey() {
        return this.hashKey;
    }

    public Object getRangeKey() {
        return this.rangeKey;
    }

    public AttributeValue getHashKeyAttributeValue() {
        if (this.hashKeyAttributeValue != null) {
            return this.hashKeyAttributeValue;
        }
        if (this.hashKey != null) {
            if (this.hashKey instanceof Number) {
                this.hashKeyAttributeValue = AttributeValue.builder().n(hashKey.toString()).build();
            } else {
                this.hashKeyAttributeValue = AttributeValue.builder().s(hashKey.toString()).build();
            }
            return this.hashKeyAttributeValue;
        }
        throw new IllegalArgumentException("hashKey is null");
    }

    public AttributeValue getRangeKeyAttributeValue() {
        if (this.rangeKeyAttributeValue != null) {
            return this.rangeKeyAttributeValue;
        }
        if (this.rangeKey != null) {
            if (this.rangeKey instanceof Number) {
                this.rangeKeyAttributeValue = AttributeValue.builder().n(rangeKey.toString()).build();
            } else {
                this.rangeKeyAttributeValue = AttributeValue.builder().s(rangeKey.toString()).build();
            }
            return this.rangeKeyAttributeValue;
        }
        throw new IllegalArgumentException("rangeKey is null");
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof KeyPair) {
            KeyPair keyPair = (KeyPair) obj;
            return this.hashKey.equals(keyPair.hashKey) && this.rangeKey.equals(keyPair.rangeKey);
        }
        return false;
    }

    @Override
    public int hashCode() {
        if (this.hashCode == 0) {
            this.hashCode = (this.hashKey + "_" + this.rangeKey).hashCode();
        }
        return this.hashCode;
    }
}
