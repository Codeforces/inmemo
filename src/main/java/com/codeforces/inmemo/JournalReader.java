package com.codeforces.inmemo;

import org.apache.log4j.Logger;
import org.jacuzzi.core.ArrayMap;
import org.jacuzzi.core.RowRoll;
import org.xerial.snappy.Snappy;

import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.File;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.zip.CRC32;

final class JournalReader implements AutoCloseable {
    private static final Logger logger = Logger.getLogger(JournalReader.class);
    private static final int STREAM_BUFFER_SIZE = 4 * 1024 * 1024;
    private static final long MAX_FUTURE_CREATED_AT_MILLIS = 60L * 60L * 1000L;

    private final File file;
    private final String expectedTableClassName;
    private final String expectedTableClassSpec;
    private final String tableNameForLog;
    private final long fileLength;

    private CountingInputStream countingInputStream;
    private DataInputStream inputStream;
    private Status status;
    private long blocksRead;
    private long rowsRead;
    private boolean closed;

    JournalReader(File file, Class<?> tableClass, String tableClassSpec) {
        this.file = file;
        this.expectedTableClassName = ReflectionUtil.getTableClassName(tableClass);
        this.expectedTableClassSpec = tableClassSpec;
        this.tableNameForLog = tableClass.getSimpleName();
        this.fileLength = file.isFile() ? file.length() : 0L;

        if (!file.isFile()) {
            status = Status.NO_FILE;
            logger.info("Can't read journal because of no file '" + file + "'.");
            return;
        }

        try {
            countingInputStream = new CountingInputStream(new BufferedInputStream(
                    Files.newInputStream(file.toPath()), STREAM_BUFFER_SIZE));
            inputStream = new DataInputStream(countingInputStream);
            readHeader();
        } catch (EOFException e) {
            status = Status.FORMAT_MISMATCH;
            logger.warn("Journal header is truncated [table='" + tableNameForLog + "'].");
            closeQuietly();
        } catch (Exception e) {
            status = Status.FORMAT_MISMATCH;
            logger.warn("Unable to read journal header [table='" + tableNameForLog + "'].", e);
            closeQuietly();
        }
    }

    Status getStatus() {
        return status == null ? Status.CLEAN_EOF : status;
    }

    long getRowsRead() {
        return rowsRead;
    }

    long getBlocksRead() {
        return blocksRead;
    }

    RowRoll nextBlock() {
        if (status != null || closed) {
            return null;
        }

        if (countingInputStream.getPosition() == fileLength) {
            finish(blocksRead == 0 ? Status.EMPTY : Status.CLEAN_EOF, null);
            return null;
        }

        try {
            int rowCount = inputStream.readInt();
            int rawLength = inputStream.readInt();
            long expectedCrc32 = inputStream.readLong();
            int compressedLength = inputStream.readInt();

            if (rowCount <= 0 || rowCount > JournalFormat.MAX_BLOCK_ROWS
                    || rawLength <= 0 || rawLength > JournalFormat.MAX_BLOCK_RAW_BYTES
                    || compressedLength <= 0
                    || compressedLength > JournalFormat.MAX_BLOCK_COMPRESSED_BYTES
                    || compressedLength > fileLength - countingInputStream.getPosition()
                    || compressedLength > Snappy.maxCompressedLength(rawLength)) {
                finish(Status.TRUNCATED, "Illegal journal block header");
                return null;
            }

            byte[] compressed = new byte[compressedLength];
            inputStream.readFully(compressed);

            if (!Snappy.isValidCompressedBuffer(compressed)) {
                finish(Status.TRUNCATED, "Invalid Snappy payload");
                return null;
            }

            int uncompressedLength = Snappy.uncompressedLength(compressed);
            if (uncompressedLength != rawLength) {
                finish(Status.TRUNCATED, "Unexpected uncompressed journal block length");
                return null;
            }

            byte[] raw = new byte[rawLength];
            int actualRawLength = Snappy.uncompress(compressed, 0, compressed.length, raw, 0);
            if (actualRawLength != rawLength) {
                finish(Status.TRUNCATED, "Unexpected actual uncompressed journal block length");
                return null;
            }

            CRC32 crc32 = new CRC32();
            crc32.update(raw, 0, raw.length);
            if (crc32.getValue() != expectedCrc32) {
                finish(Status.TRUNCATED, "Journal block CRC mismatch");
                return null;
            }

            ByteArrayInputStream rawInputStream = new ByteArrayInputStream(raw);
            RowRoll rowRoll = ArrayMap.readRowRoll(rawInputStream);
            if (rowRoll.size() != rowCount || rawInputStream.available() != 0) {
                finish(Status.TRUNCATED, "Journal block payload mismatch");
                return null;
            }

            blocksRead++;
            rowsRead += rowRoll.size();
            return rowRoll;
        } catch (EOFException e) {
            finish(Status.TRUNCATED, "Unexpected journal EOF", e);
        } catch (Exception e) {
            finish(Status.TRUNCATED, "Unable to read journal block", e);
        }

        return null;
    }

    static RowRoll readAll(File file, Class<?> tableClass, String tableClassSpec) {
        try (JournalReader reader = new JournalReader(file, tableClass, tableClassSpec)) {
            RowRoll result = null;
            List<String> resultKeys = null;

            RowRoll block;
            while ((block = reader.nextBlock()) != null) {
                if (block.isEmpty()) {
                    continue;
                }

                List<String> blockKeys = getKeys(block);
                if (result == null) {
                    result = block;
                    resultKeys = blockKeys;
                } else if (sameKeys(resultKeys, blockKeys)) {
                    result.add(block);
                } else if (sameKeySet(resultKeys, blockKeys)) {
                    for (int i = 0; i < block.size(); i++) {
                        result.addRow(block.getRow(i));
                    }
                } else {
                    logger.warn("Journal blocks have different column sets [table='" + tableClass.getSimpleName() + "'].");
                    List<String> unionKeys = unionKeys(resultKeys, blockKeys);
                    RowRoll rebuilt = null;
                    if (!sameKeys(resultKeys, unionKeys)) {
                        rebuilt = new RowRoll();
                        rebuilt.setKeys(unionKeys.toArray(new String[0]));
                        for (int i = 0; i < result.size(); i++) {
                            rebuilt.addRow(result.getRow(i));
                        }
                    }
                    for (int i = 0; i < block.size(); i++) {
                        if (rebuilt == null) {
                            result.addRow(block.getRow(i));
                        } else {
                            rebuilt.addRow(block.getRow(i));
                        }
                    }
                    if (rebuilt != null) {
                        result = rebuilt;
                        resultKeys = unionKeys;
                    }
                }
            }

            Status status = reader.getStatus();
            if (result == null) {
                if (status == Status.NO_FILE) {
                    return null;
                }
                if (status == Status.EMPTY
                        || status == Status.FORMAT_MISMATCH
                        || status == Status.EXPIRED
                        || status == Status.TRUNCATED) {
                    return null;
                }
            }

            if (result != null) {
                logger.info("Journal has been read [table='" + tableClass.getSimpleName()
                        + "', status=" + status
                        + ", blocks=" + reader.getBlocksRead()
                        + ", rows=" + reader.getRowsRead()
                        + ", bytes=" + file.length()
                        + "].");
            }

            return result;
        }
    }

    private void readHeader() throws IOException {
        byte[] magic = new byte[JournalFormat.MAGIC.length];
        inputStream.readFully(magic);
        if (!Arrays.equals(JournalFormat.MAGIC, magic)) {
            finish(Status.FORMAT_MISMATCH, "Unexpected journal magic");
            return;
        }

        int version = inputStream.readInt();
        if (version != JournalFormat.VERSION) {
            finish(Status.FORMAT_MISMATCH, "Unexpected journal version");
            return;
        }

        long createdAtMillis = inputStream.readLong();
        if (createdAtMillis <= 0) {
            finish(Status.FORMAT_MISMATCH, "Illegal journal createdAtMillis");
            return;
        }

        String tableClassName = readHeaderString();
        String tableClassSpec = readHeaderString();
        if (!expectedTableClassName.equals(tableClassName) || !expectedTableClassSpec.equals(tableClassSpec)) {
            finish(Status.FORMAT_MISMATCH, "Journal table class mismatch");
            return;
        }

        long nowMillis = System.currentTimeMillis();
        if (createdAtMillis > nowMillis + MAX_FUTURE_CREATED_AT_MILLIS
                || nowMillis - createdAtMillis > JournalFormat.getMaxAgeMillis(logger)) {
            expire();
        }
    }

    private String readHeaderString() throws IOException {
        int length = inputStream.readInt();
        if (length <= 0 || length > JournalFormat.MAX_HEADER_STRING_BYTES) {
            throw new IOException("Illegal journal header string length: " + length + ".");
        }

        byte[] bytes = new byte[length];
        inputStream.readFully(bytes);
        return new String(bytes, StandardCharsets.UTF_8);
    }

    private void expire() {
        status = Status.EXPIRED;
        closeQuietly();
        if (file.isFile() && !file.delete()) {
            logger.warn("Expired journal has not been deleted [file='" + file.getAbsolutePath() + "'].");
        } else {
            logger.warn("Expired journal has been deleted [file='" + file.getAbsolutePath() + "'].");
        }
    }

    private void finish(Status status, String message) {
        finish(status, message, null);
    }

    private void finish(Status status, String message, Exception e) {
        this.status = status;
        closeQuietly();
        if (message != null) {
            if (e == null) {
                logger.warn(message + " [table='" + tableNameForLog + "', status=" + status + "].");
            } else {
                logger.warn(message + " [table='" + tableNameForLog + "', status=" + status + "].", e);
            }
        }
    }

    @Override
    public void close() {
        closeQuietly();
    }

    private void closeQuietly() {
        if (closed) {
            return;
        }

        closed = true;
        if (inputStream != null) {
            try {
                inputStream.close();
            } catch (IOException e) {
                logger.error("Unable to close journal after reading [table='" + tableNameForLog + "'].", e);
            }
        }
    }

    private static List<String> getKeys(RowRoll rowRoll) {
        return new ArrayList<>(rowRoll.getRow(0).keySet());
    }

    private static boolean sameKeys(List<String> first, List<String> second) {
        return first.size() == second.size() && first.equals(second);
    }

    private static boolean sameKeySet(List<String> first, List<String> second) {
        return first.size() == second.size() && new LinkedHashSet<>(first).equals(new LinkedHashSet<>(second));
    }

    private static List<String> unionKeys(List<String> first, List<String> second) {
        Set<String> keys = new LinkedHashSet<>(first);
        keys.addAll(second);
        return new ArrayList<>(keys);
    }

    enum Status {
        NO_FILE,
        FORMAT_MISMATCH,
        EXPIRED,
        EMPTY,
        CLEAN_EOF,
        TRUNCATED
    }

    private static final class CountingInputStream extends FilterInputStream {
        private long position;

        private CountingInputStream(InputStream in) {
            super(in);
        }

        private long getPosition() {
            return position;
        }

        @Override
        public int read() throws IOException {
            int result = super.read();
            if (result != -1) {
                position++;
            }
            return result;
        }

        @Override
        public int read(byte[] b, int off, int len) throws IOException {
            int result = super.read(b, off, len);
            if (result > 0) {
                position += result;
            }
            return result;
        }

        @Override
        public long skip(long n) throws IOException {
            long result = super.skip(n);
            position += result;
            return result;
        }
    }
}
