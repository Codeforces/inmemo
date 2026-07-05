package com.codeforces.inmemo;

import org.apache.log4j.Logger;
import org.xerial.snappy.Snappy;

import java.nio.charset.StandardCharsets;

final class JournalFormat {
    static final byte[] MAGIC = "INMEMO_JOURNAL_2".getBytes(StandardCharsets.US_ASCII);
    static final int VERSION = 1;

    static final int MAX_BLOCK_ROWS = 1 << 20;
    static final int MAX_BLOCK_RAW_BYTES = 128 * 1024 * 1024;
    static final int MAX_BLOCK_COMPRESSED_BYTES = Snappy.maxCompressedLength(MAX_BLOCK_RAW_BYTES);
    static final int MAX_HEADER_STRING_BYTES = 65_536;

    static final int DEFAULT_BLOCK_ROWS = 65_536;
    static final int DEFAULT_TARGET_RAW_BYTES = 64 * 1024 * 1024;
    static final int MIN_TARGET_RAW_BYTES = 1 * 1024 * 1024;

    static final String BLOCK_ROWS_PROPERTY = "Inmemo.JournalBlockRows";
    static final String TARGET_RAW_BYTES_PROPERTY = "Inmemo.JournalBlockTargetBytes";
    static final String MAX_AGE_HOURS_PROPERTY = "Inmemo.JournalMaxAgeHours";

    private static final long DEFAULT_MAX_AGE_HOURS = 36L;

    private JournalFormat() {
        // No operations.
    }

    static int getBlockRows(Logger logger) {
        return getIntProperty(logger, BLOCK_ROWS_PROPERTY, DEFAULT_BLOCK_ROWS, 1, MAX_BLOCK_ROWS);
    }

    static int getTargetRawBytes(Logger logger) {
        return getIntProperty(logger, TARGET_RAW_BYTES_PROPERTY, DEFAULT_TARGET_RAW_BYTES,
                MIN_TARGET_RAW_BYTES, MAX_BLOCK_RAW_BYTES / 2);
    }

    static long getMaxAgeMillis(Logger logger) {
        long hours = getLongProperty(logger, MAX_AGE_HOURS_PROPERTY, DEFAULT_MAX_AGE_HOURS, 1L, 24L * 365L);
        return hours * 60L * 60L * 1000L;
    }

    private static int getIntProperty(Logger logger, String name, int defaultValue, int minValue, int maxValue) {
        String value = System.getProperty(name);
        if (value == null) {
            return defaultValue;
        }

        try {
            int parsedValue = Integer.parseInt(value.trim());
            if (parsedValue < minValue) {
                logger.warn("Property " + name + "=" + value + " is too small, using " + minValue + '.');
                return minValue;
            }
            if (parsedValue > maxValue) {
                logger.warn("Property " + name + "=" + value + " is too large, using " + maxValue + '.');
                return maxValue;
            }
            return parsedValue;
        } catch (NumberFormatException e) {
            logger.warn("Property " + name + "=" + value + " is invalid, using " + defaultValue + '.', e);
            return defaultValue;
        }
    }

    private static long getLongProperty(Logger logger, String name, long defaultValue, long minValue, long maxValue) {
        String value = System.getProperty(name);
        if (value == null) {
            return defaultValue;
        }

        try {
            long parsedValue = Long.parseLong(value.trim());
            if (parsedValue < minValue) {
                logger.warn("Property " + name + "=" + value + " is too small, using " + minValue + '.');
                return minValue;
            }
            if (parsedValue > maxValue) {
                logger.warn("Property " + name + "=" + value + " is too large, using " + maxValue + '.');
                return maxValue;
            }
            return parsedValue;
        } catch (NumberFormatException e) {
            logger.warn("Property " + name + "=" + value + " is invalid, using " + defaultValue + '.', e);
            return defaultValue;
        }
    }
}
