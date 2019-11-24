package org.athena.util;

public final class CommonUtil {
    private CommonUtil() {
    }

    public static boolean isPowerOf2(long n) {
        return n != 0 && (n & (n - 1)) == 0;
    }
}
