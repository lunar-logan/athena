package org.athena.io.block;

public enum BlockStoreVersion {
    BLOCK_STORE_SCHEMA_V1(1, "BlockStoreSchemaV1");

    private final int code;
    private final String key;

    BlockStoreVersion(int code, String key) {
        this.code = code;
        this.key = key;
    }

    public int getCode() {
        return code;
    }

    public String getKey() {
        return key;
    }
}
