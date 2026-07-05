package com.codeforces.inmemo;

import com.codeforces.inmemo.model.JournalDisabledUser;
import com.codeforces.inmemo.model.JournalEnabledUser;
import org.apache.log4j.Logger;
import org.hsqldb.jdbc.JDBCDataSource;
import org.jacuzzi.core.ArrayMap;
import org.jacuzzi.core.Row;
import org.jacuzzi.core.RowRoll;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.xerial.snappy.Snappy;
import org.xerial.snappy.SnappyInputStream;

import java.io.BufferedInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.Files;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.Arrays;
import java.util.zip.CRC32;

public class JournalV2Test {
    private static final Logger logger = Logger.getLogger(JournalV2Test.class);

    private String oldUseJournalProperty;
    private String oldBlockRowsProperty;
    private String oldTargetBytesProperty;
    private String oldMaxAgeProperty;
    private File oldJournalsDir;
    private File tempJournalsDir;

    @Before
    public void setUp() throws Exception {
        oldUseJournalProperty = System.getProperty("Inmemo.UseJournal");
        oldBlockRowsProperty = System.getProperty(JournalFormat.BLOCK_ROWS_PROPERTY);
        oldTargetBytesProperty = System.getProperty(JournalFormat.TARGET_RAW_BYTES_PROPERTY);
        oldMaxAgeProperty = System.getProperty(JournalFormat.MAX_AGE_HOURS_PROPERTY);

        System.setProperty("Inmemo.UseJournal", "true");
        System.setProperty(JournalFormat.BLOCK_ROWS_PROPERTY, "2");
        System.clearProperty(JournalFormat.TARGET_RAW_BYTES_PROPERTY);
        System.clearProperty(JournalFormat.MAX_AGE_HOURS_PROPERTY);

        oldJournalsDir = Table.getJournalsDirForTesting();
        tempJournalsDir = Files.createTempDirectory("inmemo-journals-v2-").toFile();
        Table.setJournalsDir(tempJournalsDir);
    }

    @After
    public void tearDown() {
        restoreProperty("Inmemo.UseJournal", oldUseJournalProperty);
        restoreProperty(JournalFormat.BLOCK_ROWS_PROPERTY, oldBlockRowsProperty);
        restoreProperty(JournalFormat.TARGET_RAW_BYTES_PROPERTY, oldTargetBytesProperty);
        restoreProperty(JournalFormat.MAX_AGE_HOURS_PROPERTY, oldMaxAgeProperty);

        if (oldJournalsDir != null) {
            Table.setJournalsDirForTesting(oldJournalsDir);
        }

        deleteRecursively(tempJournalsDir);
    }

    @Test
    public void testRoundTripMultiblockAndDeletesStaleTmp() throws Exception {
        File tmpFile = tmpJournalFile(JournalEnabledUser.class);
        Files.write(tmpFile.toPath(), new byte[]{1, 2, 3});

        Table<JournalEnabledUser> table = new Table<>(JournalEnabledUser.class, "id", null);
        for (long id = 1; id <= 5; id++) {
            table.insertOrUpdate(user(id, "u" + id, "u" + id + "@example.com"),
                    row(id, "u" + id, "u" + id + "@example.com"));
        }
        table.writeJournal();

        Assert.assertFalse(tmpFile.exists());
        Assert.assertTrue(journalFile(JournalEnabledUser.class).isFile());

        RowRoll rows = table.readJournal();
        Assert.assertNotNull(rows);
        Assert.assertEquals(5, rows.size());
        Assert.assertEquals("u5", rows.getRow(4).get("handle"));

        try (JournalReader reader = newReader(JournalEnabledUser.class)) {
            int blocks = 0;
            while (reader.nextBlock() != null) {
                blocks++;
            }
            Assert.assertTrue(blocks > 1);
            Assert.assertEquals(JournalReader.Status.CLEAN_EOF, reader.getStatus());
        }
    }

    @Test
    public void testEmptyFinishClosesWriter() throws Exception {
        Table<JournalEnabledUser> table = new Table<>(JournalEnabledUser.class, "id", null);
        table.writeJournal();

        Assert.assertFalse(journalFile(JournalEnabledUser.class).exists());

        table.insertOrUpdate(user(1L, "late", "late@example.com"), row(1L, "late", "late@example.com"));
        table.writeJournal();

        Assert.assertFalse(journalFile(JournalEnabledUser.class).exists());
    }

    @Test
    public void testNonEligibleTableDoesNotWriteJournal() throws Exception {
        Table<JournalEnabledUser> table = new Table<>(JournalEnabledUser.class, "id", null);
        table.setJournalEligible(false);

        table.insertOrUpdate(user(1L, "delta", "delta@example.com"), row(1L, "delta", "delta@example.com"));
        table.writeJournal();

        Assert.assertFalse(journalFile(JournalEnabledUser.class).exists());
    }

    @Test
    public void testTruncatedTailKeepsWholePrefix() throws Exception {
        writeFiveRows();

        try (RandomAccessFile file = new RandomAccessFile(journalFile(JournalEnabledUser.class), "rw")) {
            file.setLength(file.length() - 1);
        }

        try (JournalReader reader = newReader(JournalEnabledUser.class)) {
            int rows = 0;
            RowRoll block;
            while ((block = reader.nextBlock()) != null) {
                rows += block.size();
            }

            Assert.assertEquals(4, rows);
            Assert.assertEquals(JournalReader.Status.TRUNCATED, reader.getStatus());
        }
    }

    @Test
    public void testCorruptedPayloadKeepsWholePrefix() throws Exception {
        writeFiveRows();

        File file = journalFile(JournalEnabledUser.class);
        try (RandomAccessFile randomAccessFile = new RandomAccessFile(file, "rw")) {
            randomAccessFile.seek(file.length() - 1);
            int value = randomAccessFile.read();
            randomAccessFile.seek(file.length() - 1);
            randomAccessFile.write(value ^ 1);
        }

        try (JournalReader reader = newReader(JournalEnabledUser.class)) {
            int rows = 0;
            RowRoll block;
            while ((block = reader.nextBlock()) != null) {
                rows += block.size();
            }

            Assert.assertEquals(4, rows);
            Assert.assertEquals(JournalReader.Status.TRUNCATED, reader.getStatus());
        }
    }

    @Test
    public void testForeignMagicIsFormatMismatch() throws Exception {
        Files.write(journalFile(JournalEnabledUser.class).toPath(), new byte[]{1, 2, 3});

        Assert.assertNull(new Table<>(JournalEnabledUser.class, "id", null).readJournal());
        try (JournalReader reader = newReader(JournalEnabledUser.class)) {
            Assert.assertEquals(JournalReader.Status.FORMAT_MISMATCH, reader.getStatus());
        }
    }

    @Test
    public void testClassSpecMismatchIsFormatMismatch() throws Exception {
        writeFiveRows();

        try (JournalReader reader = new JournalReader(journalFile(JournalEnabledUser.class),
                JournalDisabledUser.class, ReflectionUtil.getTableClassSpec(JournalDisabledUser.class))) {
            Assert.assertEquals(JournalReader.Status.FORMAT_MISMATCH, reader.getStatus());
        }
    }

    @Test
    public void testExpiredJournalIsDeleted() throws Exception {
        writeHeaderOnlyJournal(System.currentTimeMillis() - 48L * 60L * 60L * 1000L);

        try (JournalReader reader = newReader(JournalEnabledUser.class)) {
            Assert.assertEquals(JournalReader.Status.EXPIRED, reader.getStatus());
        }

        Assert.assertFalse(journalFile(JournalEnabledUser.class).exists());
    }

    @Test
    public void testSchemaDriftReadAllUsesUnion() {
        System.setProperty(JournalFormat.BLOCK_ROWS_PROPERTY, "1");

        JournalWriter writer = new JournalWriter(journalFile(JournalEnabledUser.class),
                JournalEnabledUser.class, ReflectionUtil.getTableClassSpec(JournalEnabledUser.class));
        writer.addRow(rowWithoutEmail(1L, "first"));
        writer.addRow(row(2L, "second", "second@example.com"));
        writer.finish();

        RowRoll rows = JournalReader.readAll(journalFile(JournalEnabledUser.class),
                JournalEnabledUser.class, ReflectionUtil.getTableClassSpec(JournalEnabledUser.class));

        Assert.assertNotNull(rows);
        Assert.assertEquals(2, rows.size());
        Assert.assertNull(rows.getRow(0).get("email"));
        Assert.assertEquals("second@example.com", rows.getRow(1).get("email"));
    }

    @Test
    public void testSchemaDriftReadAllKeepsExistingUnionForSubsetBlocks() {
        System.setProperty(JournalFormat.BLOCK_ROWS_PROPERTY, "1");

        JournalWriter writer = new JournalWriter(journalFile(JournalEnabledUser.class),
                JournalEnabledUser.class, ReflectionUtil.getTableClassSpec(JournalEnabledUser.class));
        writer.addRow(row(1L, "first", "first@example.com"));
        writer.addRow(rowWithoutEmail(2L, "second"));
        writer.addRow(rowWithoutEmail(3L, "third"));
        writer.finish();

        RowRoll rows = JournalReader.readAll(journalFile(JournalEnabledUser.class),
                JournalEnabledUser.class, ReflectionUtil.getTableClassSpec(JournalEnabledUser.class));

        Assert.assertNotNull(rows);
        Assert.assertEquals(3, rows.size());
        Assert.assertEquals("first@example.com", rows.getRow(0).get("email"));
        Assert.assertNull(rows.getRow(1).get("email"));
        Assert.assertNull(rows.getRow(2).get("email"));
    }

    @Test
    public void testSameKeySetDifferentOrderReadAll() {
        System.setProperty(JournalFormat.BLOCK_ROWS_PROPERTY, "1");

        JournalWriter writer = new JournalWriter(journalFile(JournalEnabledUser.class),
                JournalEnabledUser.class, ReflectionUtil.getTableClassSpec(JournalEnabledUser.class));
        writer.addRow(row(1L, "first", "first@example.com"));
        writer.addRow(rowDifferentOrder(2L, "second", "second@example.com"));
        writer.finish();

        RowRoll rows = JournalReader.readAll(journalFile(JournalEnabledUser.class),
                JournalEnabledUser.class, ReflectionUtil.getTableClassSpec(JournalEnabledUser.class));

        Assert.assertNotNull(rows);
        Assert.assertEquals(2, rows.size());
        Assert.assertEquals("first", rows.getRow(0).get("handle"));
        Assert.assertEquals("second@example.com", rows.getRow(1).get("email"));
    }

    @Test
    public void testIllegalBlockHeadersDoNotAllocatePayload() throws Exception {
        assertIllegalBlockHeader(Integer.MAX_VALUE, 1, 1);
        assertIllegalBlockHeader(-1, 1, 1);
        assertIllegalBlockHeader(1, Integer.MAX_VALUE, 1);
        assertIllegalBlockHeader(1, -1, 1);
        assertIllegalBlockHeader(1, 1, Integer.MAX_VALUE);
        assertIllegalBlockHeader(1, 1, -1);
        assertIllegalBlockHeader(1, 16, 1000);

        int compressedLength = Snappy.maxCompressedLength(1) + 1;
        writeBlockHeaderOnly(1, 1, compressedLength);
        try (RandomAccessFile file = new RandomAccessFile(journalFile(JournalEnabledUser.class), "rw")) {
            file.setLength(file.length() + compressedLength);
        }
        assertReaderNextBlockStatus(JournalReader.Status.TRUNCATED);
    }

    @Test
    public void testIllegalHeaderStringLengthsAreFormatMismatch() throws Exception {
        writeHeaderWithIllegalClassNameLength(Integer.MAX_VALUE);
        assertReaderStatus(JournalReader.Status.FORMAT_MISMATCH);

        writeHeaderWithIllegalClassNameLength(0);
        assertReaderStatus(JournalReader.Status.FORMAT_MISMATCH);

        writeHeaderWithIllegalClassSpecLength(Integer.MAX_VALUE);
        assertReaderStatus(JournalReader.Status.FORMAT_MISMATCH);

        writeHeaderWithIllegalClassSpecLength(0);
        assertReaderStatus(JournalReader.Status.FORMAT_MISMATCH);
    }

    @Test
    public void testBlockPayloadRowCountMismatchIsTruncated() throws Exception {
        writeUpperRows(1L);
        overwriteFirstBlockRowCount(journalFile(JournalEnabledUser.class), 2);

        assertReaderNextBlockStatus(JournalReader.Status.TRUNCATED);
    }

    @Test
    public void testBlockPayloadTrailingBytesAreTruncated() throws Exception {
        RowRoll rowRoll = new RowRoll();
        rowRoll.addRow(row(1L, "first", "first@example.com"));
        writePayloadBlock(rowRoll, new byte[]{0});

        assertReaderNextBlockStatus(JournalReader.Status.TRUNCATED);
    }

    @Test
    public void testJournalCreatedTooFarInFutureIsExpired() throws Exception {
        writeHeaderOnlyJournal(System.currentTimeMillis() + 2L * 60L * 60L * 1000L);

        assertReaderStatus(JournalReader.Status.EXPIRED);
        Assert.assertFalse(journalFile(JournalEnabledUser.class).exists());
    }

    @Test
    public void testNonPositiveJournalCreatedAtIsFormatMismatch() throws Exception {
        writeHeaderOnlyJournal(0L);
        assertReaderStatus(JournalReader.Status.FORMAT_MISMATCH);
        Assert.assertTrue(journalFile(JournalEnabledUser.class).exists());

        writeHeaderOnlyJournal(-1L);
        assertReaderStatus(JournalReader.Status.FORMAT_MISMATCH);
        Assert.assertTrue(journalFile(JournalEnabledUser.class).exists());
    }

    @Test
    public void testJournalPropertiesAreClampedAndDefaulted() {
        System.setProperty(JournalFormat.BLOCK_ROWS_PROPERTY, "0");
        Assert.assertEquals(1, JournalFormat.getBlockRows(logger));
        System.setProperty(JournalFormat.BLOCK_ROWS_PROPERTY, Integer.toString(Integer.MAX_VALUE));
        Assert.assertEquals(JournalFormat.MAX_BLOCK_ROWS, JournalFormat.getBlockRows(logger));
        System.setProperty(JournalFormat.BLOCK_ROWS_PROPERTY, "bad");
        Assert.assertEquals(JournalFormat.DEFAULT_BLOCK_ROWS, JournalFormat.getBlockRows(logger));

        System.setProperty(JournalFormat.TARGET_RAW_BYTES_PROPERTY, "1");
        Assert.assertEquals(JournalFormat.MIN_TARGET_RAW_BYTES, JournalFormat.getTargetRawBytes(logger));
        System.setProperty(JournalFormat.TARGET_RAW_BYTES_PROPERTY,
                Integer.toString(JournalFormat.MAX_BLOCK_RAW_BYTES));
        Assert.assertEquals(JournalFormat.MAX_BLOCK_RAW_BYTES / 2, JournalFormat.getTargetRawBytes(logger));
        System.setProperty(JournalFormat.TARGET_RAW_BYTES_PROPERTY, "bad");
        Assert.assertEquals(JournalFormat.DEFAULT_TARGET_RAW_BYTES, JournalFormat.getTargetRawBytes(logger));

        System.setProperty(JournalFormat.MAX_AGE_HOURS_PROPERTY, "0");
        Assert.assertEquals(60L * 60L * 1000L, JournalFormat.getMaxAgeMillis(logger));
        System.setProperty(JournalFormat.MAX_AGE_HOURS_PROPERTY, Long.toString(24L * 365L + 1L));
        Assert.assertEquals(24L * 365L * 60L * 60L * 1000L, JournalFormat.getMaxAgeMillis(logger));
        System.setProperty(JournalFormat.MAX_AGE_HOURS_PROPERTY, "bad");
        Assert.assertEquals(36L * 60L * 60L * 1000L, JournalFormat.getMaxAgeMillis(logger));
    }

    @Test
    public void testUnsupportedValueDisablesWriterWithoutException() throws Exception {
        Table<JournalEnabledUser> table = new Table<>(JournalEnabledUser.class, "id", null);
        Row row = new Row(4);
        row.put("id", 1L);
        row.put("handle", "bad");
        row.put("email", "bad@example.com");
        row.put("payload", new byte[]{1, 2, 3});

        table.insertOrUpdate(user(1L, "bad", "bad@example.com"), row);
        table.writeJournal();

        Assert.assertFalse(journalFile(JournalEnabledUser.class).exists());
        Assert.assertFalse(tmpJournalFile(JournalEnabledUser.class).exists());
    }

    @Test
    public void testReplayBlocksThenAppendSqlDelta() throws Exception {
        createDataSourceWithRows(1L, 2L, 3L, 4L, 5L);
        writeUpperRows(1L, 2L, 3L);

        File file = journalFile(JournalEnabledUser.class);
        byte[] originalBytes = Files.readAllBytes(file.toPath());
        long originalCreatedAtMillis = readCreatedAtMillis(file);

        Table<JournalEnabledUser> table = createUpdaterTable();
        TableUpdater<JournalEnabledUser> updater = table.getTableUpdaterForTesting();

        TableUpdater.UpdateResult firstBlock = updater.internalUpdate();
        Assert.assertTrue(firstBlock.journalReplayInProgress);
        Assert.assertEquals(2, table.size());
        Assert.assertFalse(table.isPreloaded());

        TableUpdater.UpdateResult secondBlock = updater.internalUpdate();
        Assert.assertTrue(secondBlock.journalReplayInProgress);
        Assert.assertEquals(3, table.size());
        Assert.assertFalse(table.isPreloaded());

        TableUpdater.UpdateResult sqlDelta = updater.internalUpdate();
        Assert.assertFalse(sqlDelta.journalReplayInProgress);
        Assert.assertEquals(5, table.size());
        Assert.assertFalse(table.isPreloaded());

        TableUpdater.UpdateResult finish = updater.internalUpdate();
        Assert.assertFalse(finish.journalReplayInProgress);
        Assert.assertTrue(table.isPreloaded());

        byte[] appendedBytes = Files.readAllBytes(file.toPath());
        Assert.assertTrue(appendedBytes.length > originalBytes.length);
        Assert.assertTrue(startsWith(appendedBytes, originalBytes));
        Assert.assertEquals(originalCreatedAtMillis, readCreatedAtMillis(file));

        RowRoll rows = JournalReader.readAll(file,
                JournalEnabledUser.class, ReflectionUtil.getTableClassSpec(JournalEnabledUser.class));
        Assert.assertNotNull(rows);
        Assert.assertEquals(5, rows.size());
        Assert.assertEquals(5L, rows.getRow(4).get("ID"));
    }

    @Test
    public void testRandomizedUpdateDoesNotSleepBetweenJournalBlocks() throws Exception {
        createDataSourceWithRows(1L, 2L, 3L, 4L, 5L);
        writeUpperRows(1L, 2L, 3L);

        Table<JournalEnabledUser> table = new Table<>(JournalEnabledUser.class, "ID", null);
        table.disableJournalWriter();
        RecordingTableUpdater updater = new RecordingTableUpdater(table);

        updater.randomizedUpdate();
        Assert.assertEquals(0, updater.sleepCount);
        Assert.assertEquals(2, table.size());

        updater.randomizedUpdate();
        Assert.assertEquals(0, updater.sleepCount);
        Assert.assertEquals(3, table.size());

        updater.randomizedUpdate();
        Assert.assertEquals(1, updater.sleepCount);
        Assert.assertEquals(5, table.size());

        table.writeJournal();
    }

    @Test
    public void testUpdaterWithInitialIndicatorValueDoesNotWriteJournal() throws Exception {
        createDataSourceWithRows(1L, 2L);

        Table<JournalEnabledUser> table = new Table<>(JournalEnabledUser.class, "ID", null);
        table.createUpdater(1L);
        runUpdaterUntilPreloaded(table, 4);

        Assert.assertFalse(journalFile(JournalEnabledUser.class).exists());
        Assert.assertFalse(tmpJournalFile(JournalEnabledUser.class).exists());
    }

    @Test
    public void testTruncatedTailWithPrefixDoesNotRewriteJournal() throws Exception {
        createDataSourceWithRows(1L, 2L, 3L, 4L, 5L, 6L);
        writeUpperRows(1L, 2L, 3L, 4L, 5L);

        File file = journalFile(JournalEnabledUser.class);
        try (RandomAccessFile randomAccessFile = new RandomAccessFile(file, "rw")) {
            randomAccessFile.setLength(randomAccessFile.length() - 1);
        }
        byte[] truncatedBytes = Files.readAllBytes(file.toPath());

        Table<JournalEnabledUser> table = createUpdaterTable();
        runUpdaterUntilPreloaded(table, 8);

        Assert.assertEquals(6, table.size());
        Assert.assertArrayEquals(truncatedBytes, Files.readAllBytes(file.toPath()));

        RowRoll rows = JournalReader.readAll(file,
                JournalEnabledUser.class, ReflectionUtil.getTableClassSpec(JournalEnabledUser.class));
        Assert.assertNotNull(rows);
        Assert.assertEquals(4, rows.size());
    }

    @Test
    public void testTruncatedFirstBlockIsReplacedByFreshJournal() throws Exception {
        createDataSourceWithRows(1L, 2L, 3L, 4L);
        writeUpperRows(1L, 2L, 3L);

        File file = journalFile(JournalEnabledUser.class);
        overwriteFirstBlockRowCount(file, 0);
        byte[] corruptedBytes = Files.readAllBytes(file.toPath());

        Table<JournalEnabledUser> table = createUpdaterTable();
        runUpdaterUntilPreloaded(table, 6);

        byte[] healedBytes = Files.readAllBytes(file.toPath());
        Assert.assertFalse(Arrays.equals(corruptedBytes, healedBytes));

        RowRoll rows = JournalReader.readAll(file,
                JournalEnabledUser.class, ReflectionUtil.getTableClassSpec(JournalEnabledUser.class));
        Assert.assertNotNull(rows);
        Assert.assertEquals(4, rows.size());
        try (JournalReader reader = newReader(JournalEnabledUser.class)) {
            while (reader.nextBlock() != null) {
                // No operations.
            }
            Assert.assertEquals(JournalReader.Status.CLEAN_EOF, reader.getStatus());
        }
    }

    @Test
    public void testEmptyAppendFinishDoesNotTouchExistingJournal() throws Exception {
        writeUpperRows(1L, 2L);

        File file = journalFile(JournalEnabledUser.class);
        byte[] before = Files.readAllBytes(file.toPath());
        long lastModified = file.lastModified();

        JournalWriter.append(file,
                JournalEnabledUser.class, ReflectionUtil.getTableClassSpec(JournalEnabledUser.class)).finish();

        Assert.assertArrayEquals(before, Files.readAllBytes(file.toPath()));
        Assert.assertEquals(lastModified, file.lastModified());
    }

    @Test
    public void testOpeningTerminalStatusesCreateFreshJournalBeforeSql() throws Exception {
        assertTerminalStatusCreatesFreshJournal(this::deleteJournalFiles);
        assertTerminalStatusCreatesFreshJournal(() -> Files.write(journalFile(JournalEnabledUser.class).toPath(),
                new byte[]{1, 2, 3}));
        assertTerminalStatusCreatesFreshJournal(() -> writeHeaderOnlyJournal(System.currentTimeMillis()
                - 48L * 60L * 60L * 1000L));
        assertTerminalStatusCreatesFreshJournal(() -> writeHeaderOnlyJournal(System.currentTimeMillis()));
    }

    @Test
    public void testRollbackPreflightOldReaderGetsCatchableException() throws Exception {
        writeFiveRows();

        byte[] journalBytes = Files.readAllBytes(journalFile(JournalEnabledUser.class).toPath());
        try (SnappyInputStream inputStream = new SnappyInputStream(new ByteArrayInputStream(journalBytes))) {
            ArrayMap.readRowRoll(inputStream);
            Assert.fail("Old reader must not read v2 journal.");
        } catch (IOException | ClassCastException expected) {
            // Expected.
        }
    }

    private void writeFiveRows() throws Exception {
        Table<JournalEnabledUser> table = new Table<>(JournalEnabledUser.class, "id", null);
        for (long id = 1; id <= 5; id++) {
            table.insertOrUpdate(user(id, "u" + id, "u" + id + "@example.com"),
                    row(id, "u" + id, "u" + id + "@example.com"));
        }
        table.writeJournal();
    }

    private void writeUpperRows(long... ids) {
        JournalWriter writer = new JournalWriter(journalFile(JournalEnabledUser.class),
                JournalEnabledUser.class, ReflectionUtil.getTableClassSpec(JournalEnabledUser.class));
        for (long id : ids) {
            writer.addRow(upperRow(id));
        }
        writer.finish();
    }

    private void writeHeaderOnlyJournal(long createdAtMillis) throws IOException {
        try (DataOutputStream outputStream = new DataOutputStream(Files.newOutputStream(
                journalFile(JournalEnabledUser.class).toPath()))) {
            writeJournalHeader(outputStream, createdAtMillis);
        }
    }

    private void writeHeaderWithIllegalClassNameLength(int classNameLength) throws IOException {
        try (DataOutputStream outputStream = new DataOutputStream(Files.newOutputStream(
                journalFile(JournalEnabledUser.class).toPath()))) {
            outputStream.write(JournalFormat.MAGIC);
            outputStream.writeInt(JournalFormat.VERSION);
            outputStream.writeLong(System.currentTimeMillis());
            outputStream.writeInt(classNameLength);
        }
    }

    private void writeHeaderWithIllegalClassSpecLength(int classSpecLength) throws IOException {
        try (DataOutputStream outputStream = new DataOutputStream(Files.newOutputStream(
                journalFile(JournalEnabledUser.class).toPath()))) {
            outputStream.write(JournalFormat.MAGIC);
            outputStream.writeInt(JournalFormat.VERSION);
            outputStream.writeLong(System.currentTimeMillis());
            writeHeaderString(outputStream, ReflectionUtil.getTableClassName(JournalEnabledUser.class));
            outputStream.writeInt(classSpecLength);
        }
    }

    private void assertIllegalBlockHeader(int rowCount, int rawLength, int compressedLength) throws IOException {
        writeBlockHeaderOnly(rowCount, rawLength, compressedLength);
        assertReaderNextBlockStatus(JournalReader.Status.TRUNCATED);
    }

    private void writeBlockHeaderOnly(int rowCount, int rawLength, int compressedLength) throws IOException {
        try (DataOutputStream outputStream = new DataOutputStream(Files.newOutputStream(
                journalFile(JournalEnabledUser.class).toPath()))) {
            writeJournalHeader(outputStream, System.currentTimeMillis());
            outputStream.writeInt(rowCount);
            outputStream.writeInt(rawLength);
            outputStream.writeLong(0L);
            outputStream.writeInt(compressedLength);
        }
    }

    private void writePayloadBlock(RowRoll rowRoll, byte[] trailingBytes) throws IOException {
        ByteArrayOutputStream rawOutputStream = new ByteArrayOutputStream();
        ArrayMap.writeRowRoll(rawOutputStream, rowRoll);
        if (trailingBytes != null) {
            rawOutputStream.write(trailingBytes);
        }

        byte[] raw = rawOutputStream.toByteArray();
        byte[] compressed = new byte[Snappy.maxCompressedLength(raw.length)];
        int compressedLength = Snappy.compress(raw, 0, raw.length, compressed, 0);

        CRC32 crc32 = new CRC32();
        crc32.update(raw);

        try (DataOutputStream outputStream = new DataOutputStream(Files.newOutputStream(
                journalFile(JournalEnabledUser.class).toPath()))) {
            writeJournalHeader(outputStream, System.currentTimeMillis());
            outputStream.writeInt(rowRoll.size());
            outputStream.writeInt(raw.length);
            outputStream.writeLong(crc32.getValue());
            outputStream.writeInt(compressedLength);
            outputStream.write(compressed, 0, compressedLength);
        }
    }

    private void writeJournalHeader(DataOutputStream outputStream, long createdAtMillis) throws IOException {
        outputStream.write(JournalFormat.MAGIC);
        outputStream.writeInt(JournalFormat.VERSION);
        outputStream.writeLong(createdAtMillis);
        writeHeaderString(outputStream, ReflectionUtil.getTableClassName(JournalEnabledUser.class));
        writeHeaderString(outputStream, ReflectionUtil.getTableClassSpec(JournalEnabledUser.class));
    }

    private void createDataSourceWithRows(long... ids) throws Exception {
        JDBCDataSource dataSource = new JDBCDataSource();
        dataSource.setUrl("jdbc:hsqldb:mem:journal-v2-" + System.nanoTime());
        dataSource.setUser("sa");
        dataSource.setPassword("");

        try (Connection connection = dataSource.getConnection()) {
            connection.createStatement().execute("DROP TABLE JournalEnabledUser IF EXISTS");
            connection.createStatement().execute("CREATE TABLE JournalEnabledUser ("
                    + "ID BIGINT, "
                    + "HANDLE VARCHAR(255), "
                    + "EMAIL VARCHAR(255))");

            try (PreparedStatement statement = connection.prepareStatement(
                    "INSERT INTO JournalEnabledUser (ID, HANDLE, EMAIL) VALUES (?, ?, ?)")) {
                for (long id : ids) {
                    statement.setLong(1, id);
                    statement.setString(2, "u" + id);
                    statement.setString(3, "u" + id + "@example.com");
                    statement.executeUpdate();
                }
            }
        }

        TableUpdater.setDataSource(dataSource);
    }

    private Table<JournalEnabledUser> createUpdaterTable() {
        Table<JournalEnabledUser> table = new Table<>(JournalEnabledUser.class, "ID", null);
        table.createUpdater(null);
        return table;
    }

    private void runUpdaterUntilPreloaded(Table<JournalEnabledUser> table, int maxIterations) {
        TableUpdater<JournalEnabledUser> updater = table.getTableUpdaterForTesting();
        for (int i = 0; i < maxIterations; i++) {
            updater.internalUpdate();
            if (table.isPreloaded()) {
                return;
            }
        }
        Assert.fail("Table has not been preloaded after " + maxIterations + " update iterations.");
    }

    private void assertTerminalStatusCreatesFreshJournal(ThrowingRunnable fileSetup) throws Exception {
        deleteJournalFiles();
        fileSetup.run();
        createDataSourceWithRows(1L, 2L);

        File file = journalFile(JournalEnabledUser.class);
        Table<JournalEnabledUser> table = createUpdaterTable();
        runUpdaterUntilPreloaded(table, 4);

        Assert.assertTrue(file.isFile());
        RowRoll rows = JournalReader.readAll(file,
                JournalEnabledUser.class, ReflectionUtil.getTableClassSpec(JournalEnabledUser.class));
        Assert.assertNotNull(rows);
        Assert.assertEquals(2, rows.size());
        try (JournalReader reader = newReader(JournalEnabledUser.class)) {
            while (reader.nextBlock() != null) {
                // No operations.
            }
            Assert.assertEquals(JournalReader.Status.CLEAN_EOF, reader.getStatus());
        }
    }

    private void deleteJournalFiles() throws IOException {
        Files.deleteIfExists(journalFile(JournalEnabledUser.class).toPath());
        Files.deleteIfExists(tmpJournalFile(JournalEnabledUser.class).toPath());
    }

    private static boolean startsWith(byte[] bytes, byte[] prefix) {
        if (bytes.length < prefix.length) {
            return false;
        }

        for (int i = 0; i < prefix.length; i++) {
            if (bytes[i] != prefix[i]) {
                return false;
            }
        }

        return true;
    }

    private static long readCreatedAtMillis(File file) throws IOException {
        try (DataInputStream inputStream = new DataInputStream(new BufferedInputStream(
                Files.newInputStream(file.toPath())))) {
            byte[] magic = new byte[JournalFormat.MAGIC.length];
            inputStream.readFully(magic);
            Assert.assertArrayEquals(JournalFormat.MAGIC, magic);
            Assert.assertEquals(JournalFormat.VERSION, inputStream.readInt());
            return inputStream.readLong();
        }
    }

    private void assertReaderStatus(JournalReader.Status expectedStatus) {
        try (JournalReader reader = newReader(JournalEnabledUser.class)) {
            Assert.assertEquals(expectedStatus, reader.getStatus());
        }
    }

    private void assertReaderNextBlockStatus(JournalReader.Status expectedStatus) {
        try (JournalReader reader = newReader(JournalEnabledUser.class)) {
            Assert.assertNull(reader.nextBlock());
            Assert.assertEquals(expectedStatus, reader.getStatus());
        }
    }

    private static void overwriteFirstBlockRowCount(File file, int rowCount) throws IOException {
        try (RandomAccessFile randomAccessFile = new RandomAccessFile(file, "rw")) {
            randomAccessFile.seek(getFirstBlockOffset(file));
            randomAccessFile.writeInt(rowCount);
        }
    }

    private static long getFirstBlockOffset(File file) throws IOException {
        long offset = JournalFormat.MAGIC.length + 4L + 8L;
        try (DataInputStream inputStream = new DataInputStream(new BufferedInputStream(
                Files.newInputStream(file.toPath())))) {
            skipFully(inputStream, JournalFormat.MAGIC.length + 4 + 8);
            int tableClassNameLength = inputStream.readInt();
            offset += 4L + tableClassNameLength;
            skipFully(inputStream, tableClassNameLength);
            int tableClassSpecLength = inputStream.readInt();
            offset += 4L + tableClassSpecLength;
        }
        return offset;
    }

    private static void skipFully(DataInputStream inputStream, int bytes) throws IOException {
        int skipped = 0;
        while (skipped < bytes) {
            int current = inputStream.skipBytes(bytes - skipped);
            if (current <= 0) {
                throw new IOException("Unexpected EOF while skipping " + bytes + " bytes.");
            }
            skipped += current;
        }
    }

    private void writeHeaderString(DataOutputStream outputStream, String value) throws IOException {
        byte[] bytes = value.getBytes("UTF-8");
        outputStream.writeInt(bytes.length);
        outputStream.write(bytes);
    }

    private JournalReader newReader(Class<?> clazz) {
        return new JournalReader(journalFile(clazz), clazz, ReflectionUtil.getTableClassSpec(clazz));
    }

    private static File journalFile(Class<?> clazz) {
        return new File(Table.getJournalsDirForTesting(), clazz.getSimpleName() + ".inmemo");
    }

    private static File tmpJournalFile(Class<?> clazz) {
        return new File(Table.getJournalsDirForTesting(), clazz.getSimpleName() + ".inmemo.tmp");
    }

    private static JournalEnabledUser user(long id, String handle, String email) {
        JournalEnabledUser user = new JournalEnabledUser();
        user.setId(id);
        user.setHandle(handle);
        user.setEmail(email);
        return user;
    }

    private static Row row(long id, String handle, String email) {
        Row row = new Row(3);
        row.put("id", id);
        row.put("handle", handle);
        row.put("email", email);
        return row;
    }

    private static Row rowDifferentOrder(long id, String handle, String email) {
        Row row = new Row(3);
        row.put("email", email);
        row.put("handle", handle);
        row.put("id", id);
        return row;
    }

    private static Row upperRow(long id) {
        Row row = new Row(3);
        row.put("ID", id);
        row.put("HANDLE", "u" + id);
        row.put("EMAIL", "u" + id + "@example.com");
        return row;
    }

    private static Row rowWithoutEmail(long id, String handle) {
        Row row = new Row(2);
        row.put("id", id);
        row.put("handle", handle);
        return row;
    }

    private static void restoreProperty(String name, String value) {
        if (value == null) {
            System.clearProperty(name);
        } else {
            System.setProperty(name, value);
        }
    }

    private static void deleteRecursively(File file) {
        if (file == null || !file.exists()) {
            return;
        }

        if (file.isDirectory()) {
            File[] files = file.listFiles();
            if (files != null) {
                for (File child : files) {
                    deleteRecursively(child);
                }
            }
        }

        if (!file.delete() && file.exists()) {
            throw new AssertionError("Can't delete " + file.getAbsolutePath());
        }
    }

    private interface ThrowingRunnable {
        void run() throws Exception;
    }

    private static final class RecordingTableUpdater extends TableUpdater<JournalEnabledUser> {
        private int sleepCount;

        private RecordingTableUpdater(Table<JournalEnabledUser> table) {
            super(table, null);
        }

        @Override
        void sleep(long timeMillis) {
            sleepCount++;
        }
    }
}
