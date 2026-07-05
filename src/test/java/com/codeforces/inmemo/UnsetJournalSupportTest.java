package com.codeforces.inmemo;

import com.codeforces.inmemo.model.JournalDisabledUser;
import com.codeforces.inmemo.model.JournalEnabledUser;
import com.codeforces.inmemo.model.JournalLateUnsetUser;
import org.jacuzzi.core.Row;
import org.jacuzzi.core.RowRoll;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.nio.file.Files;

public class UnsetJournalSupportTest {
    private String oldUseJournalProperty;
    private File oldJournalsDir;
    private File tempJournalsDir;

    @Before
    public void setUp() throws Exception {
        oldUseJournalProperty = System.getProperty("Inmemo.UseJournal");
        System.setProperty("Inmemo.UseJournal", "true");
        oldJournalsDir = Table.getJournalsDirForTesting();
        tempJournalsDir = Files.createTempDirectory("inmemo-journals-").toFile();
        Table.setJournalsDir(tempJournalsDir);
    }

    @After
    public void tearDown() {
        if (oldUseJournalProperty == null) {
            System.clearProperty("Inmemo.UseJournal");
        } else {
            System.setProperty("Inmemo.UseJournal", oldUseJournalProperty);
        }

        if (oldJournalsDir != null) {
            Table.setJournalsDirForTesting(oldJournalsDir);
        }

        deleteRecursively(tempJournalsDir);
    }

    @Test
    public void testUnsetDisablesJournal() throws Exception {
        Inmemo.unsetJournalSupport(JournalDisabledUser.class);
        Table<JournalDisabledUser> table = new Table<>(JournalDisabledUser.class, "id", null);

        Assert.assertFalse(table.isUseJournal());
        table.insertOrUpdate(user(1L, "disabled", "disabled@example.com"),
                row(1L, "disabled", "disabled@example.com"));
        table.writeJournal();

        Assert.assertFalse(journalFile(JournalDisabledUser.class).exists());
        Assert.assertNull(table.readJournal());
    }

    @Test
    public void testStaleJournalFileDeletedOnConstruction() throws Exception {
        Inmemo.unsetJournalSupport(JournalDisabledUser.class);
        File journalFile = journalFile(JournalDisabledUser.class);
        Files.write(journalFile.toPath(), new byte[]{1, 2, 3});

        Assert.assertTrue(journalFile.isFile());
        new Table<>(JournalDisabledUser.class, "id", null);

        Assert.assertFalse(journalFile.exists());
    }

    @Test
    public void testDefaultTableStillJournals() throws Exception {
        Table<JournalEnabledUser> table = new Table<>(JournalEnabledUser.class, "id", null);

        Assert.assertTrue(table.isUseJournal());
        table.insertOrUpdate(enabledUser(2L, "enabled", "enabled@example.com"),
                row(2L, "enabled", "enabled@example.com"));
        table.writeJournal();

        Assert.assertTrue(journalFile(JournalEnabledUser.class).isFile());
        RowRoll rows = table.readJournal();
        Assert.assertNotNull(rows);
        Assert.assertEquals(1, rows.size());

        Row savedRow = rows.getRow(0);
        Assert.assertEquals(2L, ((Number) savedRow.get("id")).longValue());
        Assert.assertEquals("enabled", savedRow.get("handle"));
        Assert.assertEquals("enabled@example.com", savedRow.get("email"));
    }

    @Test
    public void testUnsetAfterCreateTableThrows() {
        try {
            Inmemo.putTableForUnsetJournalSupportTestOnly(JournalLateUnsetUser.class);
            Inmemo.unsetJournalSupport(JournalLateUnsetUser.class);
            Assert.fail("Late first unsetJournalSupport call must fail.");
        } catch (IllegalStateException expected) {
            // Expected.
        } finally {
            Inmemo.removeTableForUnsetJournalSupportTestOnly(JournalLateUnsetUser.class);
        }
    }

    @Test
    public void testGlobalOffAndUnsetCompose() throws Exception {
        System.clearProperty("Inmemo.UseJournal");
        Inmemo.unsetJournalSupport(JournalDisabledUser.class);
        File staleJournalFile = journalFile(JournalDisabledUser.class);
        Files.write(staleJournalFile.toPath(), new byte[]{1, 2, 3});

        Table<JournalDisabledUser> disabledTable = new Table<>(JournalDisabledUser.class, "id", null);
        Table<JournalEnabledUser> enabledTable = new Table<>(JournalEnabledUser.class, "id", null);

        Assert.assertFalse(disabledTable.isUseJournal());
        Assert.assertFalse(staleJournalFile.exists());
        Assert.assertFalse(enabledTable.isUseJournal());
    }

    @Test
    public void testUnsetIsIdempotent() {
        Inmemo.unsetJournalSupport(JournalDisabledUser.class);
        Inmemo.unsetJournalSupport(JournalDisabledUser.class);

        try {
            Inmemo.putTableForUnsetJournalSupportTestOnly(JournalDisabledUser.class);
            Inmemo.unsetJournalSupport(JournalDisabledUser.class);
        } finally {
            Inmemo.removeTableForUnsetJournalSupportTestOnly(JournalDisabledUser.class);
        }
    }

    private static File journalFile(Class<?> clazz) {
        return new File(Table.getJournalsDirForTesting(), clazz.getSimpleName() + ".inmemo");
    }

    private static JournalDisabledUser user(long id, String handle, String email) {
        JournalDisabledUser user = new JournalDisabledUser();
        user.setId(id);
        user.setHandle(handle);
        user.setEmail(email);
        return user;
    }

    private static JournalEnabledUser enabledUser(long id, String handle, String email) {
        JournalEnabledUser user = new JournalEnabledUser();
        user.setId(id);
        user.setHandle(handle);
        user.setEmail(email);
        return user;
    }

    private static Row row(long id, String handle, String email) {
        Row row = new Row();
        row.put("id", id);
        row.put("handle", handle);
        row.put("email", email);
        return row;
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
}
