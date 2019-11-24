package org.athena.db;

public class MemtableValue {
    private final int valueLength;
    private final long offset;

    public MemtableValue(int valueLength, long offset) {
        this.valueLength = valueLength;
        this.offset = offset;
    }

    public int getValueLength() {
        return valueLength;
    }

    public long getOffset() {
        return offset;
    }

    @Override
    public String toString() {
        return "MemtableValue{" +
                "valueLength=" + valueLength +
                ", offset=" + offset +
                '}';
    }
}
