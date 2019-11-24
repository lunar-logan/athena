package org.athena.io.block.header;

import java.util.BitSet;

public class Header {
    private static final long KB = 1024L;
    private static final long MB = 1024 * KB;
    private static final long GB = 1024 * MB;
    private static final long TB = 1024 * GB;

    public static final int MAGIC_CONSTANT = 0x466c;

    private static final long DEFAULT_BLOCK_SIZE = 16 * KB;

    private final int magic;
    private final int version;
    private final long blockSize;
    private final long storeLimit;
    private final BitSet allocationTable;
    private long updatedAt;
    private int position;


    public Header(int magic, int version, long blockSize, long storeLimit, BitSet allocationTable, long updatedAt, int position) {
        this.magic = magic;
        this.version = version;
        this.blockSize = blockSize;
        this.storeLimit = storeLimit;
        this.allocationTable = allocationTable;
        this.updatedAt = updatedAt;
        this.position = position;
    }

    public int size() {
        return (allocationTable.size() >> 3) + 36;
    }

    public int getMagic() {
        return magic;
    }

    public int getVersion() {
        return version;
    }

    public long getBlockSize() {
        return blockSize;
    }

    public long getStoreLimit() {
        return storeLimit;
    }

    public BitSet getAllocationTable() {
        return allocationTable;
    }

    public long getUpdatedAt() {
        return updatedAt;
    }

    public void setUpdatedAt(long updatedAt) {
        this.updatedAt = updatedAt;
    }

    public int getPosition() {
        return position;
    }

    public void setPosition(int position) {
        this.position = position;
    }


//    private static int getAllocationTableSizeInBytes(long blockSize, long limit) {
//        return (int) ((limit / blockSize) >> 3);
//    }

//    public static Header read(ByteBuffer buf) {
//        if (buf.getInt() == MAGIC_CONSTANT) {
//            buf.rewind();
//            return readInternal(buf);
//        }
//
//        buf.rewind();
//        return null;
//    }

//    public static Header valueOf(long limit) {
//        return valueOf(DEFAULT_BLOCK_SIZE, limit);
//    }

//    public static Header valueOf(long blockSize, long limit) {
//        if (blockSize <= 0 || !CommonUtil.isPowerOf2(blockSize)) {
//            throw new IllegalArgumentException();
//        }
//        if (limit <= 0 || !CommonUtil.isPowerOf2(limit) || limit < blockSize) {
//            throw new IllegalArgumentException();
//        }
//
//        int allocationTableSize = getAllocationTableSizeInBytes(blockSize, limit);
//        byte[] allocationTable = new byte[allocationTableSize];
//        Arrays.fill(allocationTable, (byte) 0);
//
//        Header header = new Header(MAGIC_CONSTANT, 0, blockSize, limit, BitSet.valueOf(allocationTable), 0L, 0);
//        header.setPosition(header.size());
//        return header;
//    }

    @Override
    public String toString() {
        return "Header{" +
                "magic=" + magic +
                ", version=" + version +
                ", blockSize=" + blockSize +
                ", storeLimit=" + storeLimit +
                ", allocationTable=" + allocationTable +
                ", updatedAt=" + updatedAt +
                ", position=" + position +
                '}';
    }
}
