package com.codeforces.inmemo;

import org.apache.log4j.Logger;
import org.jacuzzi.core.ArrayMap;
import org.jacuzzi.core.Row;
import org.jacuzzi.core.RowRoll;
import org.xerial.snappy.Snappy;

import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.Date;
import java.util.Map;
import java.util.zip.CRC32;

final class JournalWriter {
    private static final Logger logger = Logger.getLogger(JournalWriter.class);
    private static final int STREAM_BUFFER_SIZE = 4 * 1024 * 1024;

    private final File file;
    private final File tmpFile;
    private final String tableClassName;
    private final String tableClassSpec;
    private final String tableNameForLog;
    private final int blockRows;
    private final int targetRawBytes;

    private DataOutputStream outputStream;
    private RowRoll buffer = new RowRoll();
    private DirectByteArrayOutputStream rawOutputStream;
    private byte[] compressedBuffer;

    private long estimatedRawBytes;
    private long blocks;
    private long rows;
    private long maxRawBytes;
    private long maxCompressedBytes;
    private long maxFlushMillis;
    private final long startTimeMillis = System.currentTimeMillis();

    private boolean opened;
    private boolean closed;
    private boolean failed;

    JournalWriter(File file, Class<?> tableClass, String tableClassSpec) {
        this.file = file;
        this.tmpFile = new File(file.getParentFile(), file.getName() + ".tmp");
        this.tableClassName = ReflectionUtil.getTableClassName(tableClass);
        this.tableClassSpec = tableClassSpec;
        this.tableNameForLog = tableClass.getSimpleName();
        this.blockRows = JournalFormat.getBlockRows(logger);
        this.targetRawBytes = JournalFormat.getTargetRawBytes(logger);
    }

    void addRow(Row row) {
        if (closed || failed || row == null) {
            return;
        }

        try {
            openIfNeeded();
            if (failed) {
                return;
            }

            buffer.addRow(row);
            estimatedRawBytes += estimateRawBytes(row);

            if (buffer.size() >= blockRows || estimatedRawBytes >= targetRawBytes) {
                flushBlock();
            }
        } catch (Exception e) {
            fail("Failed to add row to journal", e);
        }
    }

    void finish() {
        if (closed) {
            return;
        }

        try {
            if (!failed && buffer != null && !buffer.isEmpty()) {
                flushBlock();
            }

            if (failed) {
                closed = true;
                return;
            }

            if (!opened) {
                closed = true;
                return;
            }

            closeOutputStreamQuietly();

            if (rows == 0) {
                deleteTmpQuietly();
                closed = true;
                return;
            }

            moveTmpToTarget();
            closed = true;

            logger.info("Journal has been written [table='" + tableNameForLog
                    + "', blocks=" + blocks
                    + ", rows=" + rows
                    + ", maxRawBytes=" + maxRawBytes
                    + ", maxCompressedBytes=" + maxCompressedBytes
                    + ", maxFlushMillis=" + maxFlushMillis
                    + ", fileBytes=" + file.length()
                    + ", durationMillis=" + (System.currentTimeMillis() - startTimeMillis)
                    + "].");
        } catch (Exception e) {
            fail("Failed to finish journal", e);
            closed = true;
        }
    }

    private void openIfNeeded() throws IOException {
        if (opened) {
            return;
        }

        if (tmpFile.exists() && !tmpFile.delete()) {
            fail("Can't delete stale temporary journal file '" + tmpFile.getAbsolutePath() + "'", null);
            return;
        }

        outputStream = new DataOutputStream(new BufferedOutputStream(Files.newOutputStream(tmpFile.toPath()),
                STREAM_BUFFER_SIZE));
        writeHeader();
        opened = true;
    }

    private void writeHeader() throws IOException {
        outputStream.write(JournalFormat.MAGIC);
        outputStream.writeInt(JournalFormat.VERSION);
        outputStream.writeLong(System.currentTimeMillis());
        writeHeaderString(tableClassName);
        writeHeaderString(tableClassSpec);
    }

    private void writeHeaderString(String value) throws IOException {
        byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
        if (bytes.length == 0 || bytes.length > JournalFormat.MAX_HEADER_STRING_BYTES) {
            throw new IOException("Illegal journal header string length: " + bytes.length + ".");
        }
        outputStream.writeInt(bytes.length);
        outputStream.write(bytes);
    }

    private void flushBlock() throws IOException {
        if (buffer == null || buffer.isEmpty()) {
            return;
        }

        long startTimeMillis = System.currentTimeMillis();
        int rowCount = buffer.size();

        if (rawOutputStream == null) {
            rawOutputStream = new DirectByteArrayOutputStream(estimateRawBufferSize());
        }
        rawOutputStream.reset();
        rawOutputStream.ensureCapacity(estimateRawBufferSize());

        ArrayMap.writeRowRoll(rawOutputStream, buffer);
        int rawLength = rawOutputStream.size();
        if (rawLength <= 0 || rawLength > JournalFormat.MAX_BLOCK_RAW_BYTES) {
            throw new IOException("Journal block raw length is out of bounds [table='" + tableNameForLog
                    + "', rawLength=" + rawLength + "].");
        }

        int maxCompressedLength = Snappy.maxCompressedLength(rawLength);
        if (compressedBuffer == null || compressedBuffer.length < maxCompressedLength) {
            compressedBuffer = new byte[maxCompressedLength];
        }

        byte[] rawBuffer = rawOutputStream.getBuffer();
        CRC32 crc32 = new CRC32();
        crc32.update(rawBuffer, 0, rawLength);

        int compressedLength = Snappy.compress(rawBuffer, 0, rawLength, compressedBuffer, 0);
        if (compressedLength <= 0 || compressedLength > JournalFormat.MAX_BLOCK_COMPRESSED_BYTES) {
            throw new IOException("Journal block compressed length is out of bounds [table='" + tableNameForLog
                    + "', compressedLength=" + compressedLength + "].");
        }

        outputStream.writeInt(rowCount);
        outputStream.writeInt(rawLength);
        outputStream.writeLong(crc32.getValue());
        outputStream.writeInt(compressedLength);
        outputStream.write(compressedBuffer, 0, compressedLength);

        blocks++;
        rows += rowCount;
        maxRawBytes = Math.max(maxRawBytes, rawLength);
        maxCompressedBytes = Math.max(maxCompressedBytes, compressedLength);
        maxFlushMillis = Math.max(maxFlushMillis, System.currentTimeMillis() - startTimeMillis);

        buffer = new RowRoll();
        estimatedRawBytes = 0;
    }

    private int estimateRawBufferSize() {
        long size = estimatedRawBytes + estimatedRawBytes / 2 + 1024;
        if (size < 1024) {
            size = 1024;
        }
        if (size > JournalFormat.MAX_BLOCK_RAW_BYTES) {
            size = JournalFormat.MAX_BLOCK_RAW_BYTES;
        }
        return (int) size;
    }

    private long estimateRawBytes(Row row) {
        long result = 16;
        for (Map.Entry<String, Object> entry : row.entrySet()) {
            result += estimateString(entry.getKey());
            Object value = entry.getValue();
            if (value == null) {
                result += 1;
            } else if (value instanceof String) {
                result += estimateString((String) value);
            } else if (value instanceof Date
                    || value instanceof Byte
                    || value instanceof Integer
                    || value instanceof Long
                    || value instanceof Double
                    || value instanceof Boolean) {
                result += 12;
            } else {
                result += 12;
            }
        }
        return result;
    }

    private long estimateString(String value) {
        return value == null ? 1 : 4L + 2L * value.length();
    }

    private void moveTmpToTarget() throws IOException {
        try {
            Files.move(tmpFile.toPath(), file.toPath(),
                    StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
        } catch (AtomicMoveNotSupportedException e) {
            Files.move(tmpFile.toPath(), file.toPath(), StandardCopyOption.REPLACE_EXISTING);
        }
    }

    private void fail(String message, Exception e) {
        failed = true;
        closeOutputStreamQuietly();
        deleteTmpQuietly();
        if (e == null) {
            logger.error(message + " [table='" + tableNameForLog + "'].");
        } else {
            logger.error(message + " [table='" + tableNameForLog + "'].", e);
        }
    }

    private void closeOutputStreamQuietly() {
        if (outputStream != null) {
            try {
                outputStream.close();
            } catch (IOException e) {
                logger.error("Can't close journal output stream [table='" + tableNameForLog + "'].", e);
            } finally {
                outputStream = null;
            }
        }
    }

    private void deleteTmpQuietly() {
        if (tmpFile.isFile() && !tmpFile.delete()) {
            logger.error("Can't delete temporary journal file '" + tmpFile.getAbsolutePath()
                    + "' [table='" + tableNameForLog + "'].");
        }
    }

    private static final class DirectByteArrayOutputStream extends ByteArrayOutputStream {
        private DirectByteArrayOutputStream(int size) {
            super(size);
        }

        private byte[] getBuffer() {
            return buf;
        }

        private void ensureCapacity(int minCapacity) {
            if (minCapacity <= buf.length) {
                return;
            }

            byte[] newBuffer = new byte[minCapacity];
            System.arraycopy(buf, 0, newBuffer, 0, count);
            buf = newBuffer;
        }
    }
}
