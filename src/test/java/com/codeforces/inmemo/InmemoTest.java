package com.codeforces.inmemo;

import com.codeforces.inmemo.dao.UserDao;
import com.codeforces.inmemo.dao.impl.UserDaoImpl;
import com.codeforces.inmemo.model.User;
import com.codeforces.inmemo.model.Wrapper;
import com.codeforces.inmemo.model.Wrapper.a;
import com.mchange.v2.c3p0.ComboPooledDataSource;
import junit.framework.Assert;
import org.junit.Before;
import org.junit.Test;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsNot.not;

/**
 * @author Mike Mirzayanov (mirzayanovmr@gmail.com)
 */
public class InmemoTest {
    private static final long USER_COUNT = 10000;

    private UserDao userDao;

    private static DataSource newDataSource() {
        final ComboPooledDataSource comboPooledDataSource = new ComboPooledDataSource();
        comboPooledDataSource.setJdbcUrl("jdbc:hsqldb:mem:inmemo");
        comboPooledDataSource.setUser("sa");
        comboPooledDataSource.setPassword("");
        return comboPooledDataSource;
    }

    @Before
    public void setup() throws SQLException {
        final DataSource dataSource = newDataSource();

        userDao = new UserDaoImpl(dataSource);

        final Connection connection = dataSource.getConnection();
        connection.createStatement().execute("DROP TABLE User IF EXISTS");
        connection.createStatement().execute("CREATE TABLE User (" +
                "ID BIGINT GENERATED BY DEFAULT AS IDENTITY (START WITH 1), " +
                "HANDLE VARCHAR(255), " +
                "OPENID VARCHAR(255), " +
                "EMAIL VARCHAR(255), " +
                "PASSWORD VARCHAR(255), " +
                "ADMIN BOOLEAN, " +
                "CREATIONTIME TIMESTAMP, " +
                "LASTONLINETIME TIMESTAMP, " +
                "DISABLED BOOLEAN" +
                ")"
        );

        for (int i = 0; i < USER_COUNT; i++) {
            userDao.insertRandom();
        }

        Inmemo.setDataSource(dataSource);
    }

    @Test
    public void testCommon() throws InterruptedException {
        Inmemo.dropTableIfExists(User.class);

        // Test user count.
        {
            Assert.assertEquals(USER_COUNT, userDao.findAll().size());
        }

        // Create table.
        {
            Inmemo.createTable(User.class, "ID", null, new Indices.Builder<User>() {{
                add(Index.create("ID", Long.class, new IndexGetter<User, Long>() {
                    @Override
                    public Long get(final User tableItem) {
                        return tableItem.getId();
                    }
                }));

                add(Index.create("handle", String.class, new IndexGetter<User, String>() {
                    @Override
                    public String get(final User user) {
                        return user.getHandle();
                    }
                }));

                add(Index.create("FIRST_HANDLE_LETTER", String.class, new IndexGetter<User, String>() {
                    @Override
                    public String get(final User tableItem) {
                        return tableItem.getHandle().substring(0, 1);
                    }
                }));
            }}.build(), true);
        }

        // Assert size.
        {
            Assert.assertEquals(USER_COUNT, Inmemo.size(User.class));
        }

        // Exactly one user with id=123.
        {
            Assert.assertEquals(1, Inmemo.find(User.class, new IndexConstraint<>("ID", 123L), new Matcher<User>() {
                @Override
                public boolean match(final User user) {
                    return true;
                }
            }).size());
        }

        // Tests that there is no user[id=USER_COUNT + 1], inserts it, waits, tests that the table contains it.
        {
            Assert.assertEquals(0, Inmemo.find(User.class, new IndexConstraint<>("ID", USER_COUNT + 1), new Matcher<User>() {
                @Override
                public boolean match(final User user) {
                    return true;
                }
            }).size());

            userDao.insertRandom();

            Thread.sleep(500);

            Assert.assertEquals(1, Inmemo.find(User.class, new IndexConstraint<>("ID", USER_COUNT + 1), new Matcher<User>() {
                @Override
                public boolean match(final User user) {
                    return true;
                }
            }).size());
        }

        // Tests using second index that the number of users with first letter 'e' in handle is expected.
        {
            final long eUsers1 = Inmemo.findCount(User.class, new IndexConstraint<>("FIRST_HANDLE_LETTER", "e"), new Matcher<User>() {
                @Override
                public boolean match(final User user) {
                    return true;
                }
            });

            final long eUsers2 = Inmemo.find(User.class, new IndexConstraint<>("FIRST_HANDLE_LETTER", "e"), new Matcher<User>() {
                @Override
                public boolean match(final User user) {
                    return true;
                }
            }).size();

            Assert.assertEquals(eUsers1, eUsers2);
            Assert.assertTrue(USER_COUNT / 26 / 2 <= eUsers1 && eUsers1 <= USER_COUNT / 26 * 2);
        }

        // Tests matcher.
        {
            final long xyUsers = Inmemo.findCount(User.class, new IndexConstraint<>("FIRST_HANDLE_LETTER", "x"), new Matcher<User>() {
                @Override
                public boolean match(final User user) {
                    return user.getHandle().charAt(1) == 'y';
                }
            });

            Assert.assertTrue(USER_COUNT / 26 / 26 / 3 <= xyUsers && xyUsers <= USER_COUNT / 26 / 26 * 3);
        }

        // Tests that if we change user state in memory, it will no affect table unless we use insertOrUpdate.
        {
            User user13 = Inmemo.find(User.class, new IndexConstraint<>("ID", 13L), new Matcher<User>() {
                @Override
                public boolean match(final User user) {
                    return true;
                }
            }).get(0);

            user13.setHandle("handle13");

            org.junit.Assert.assertThat(user13.getHandle(), not(Inmemo.find(User.class, new IndexConstraint<>("ID", 13L), new Matcher<User>() {
                @Override
                public boolean match(final User user) {
                    return true;
                }
            }).get(0).getHandle()));

            Inmemo.insertOrUpdate(user13);

            org.junit.Assert.assertThat(user13.getHandle(), is(Inmemo.find(User.class, new IndexConstraint<>("ID", 13L), new Matcher<User>() {
                @Override
                public boolean match(final User user) {
                    return true;
                }
            }).get(0).getHandle()));
        }

        // Tests that findOnly throws exception if non-unique and throwOnNotUnique parameter is true;
        {
            boolean hasException = false;
            User existingUser = null;
            try {
                existingUser = Inmemo.findOnly(true, User.class, new IndexConstraint<>("ID", USER_COUNT / 5));
            } catch (InmemoException e) {
                hasException = true;
            }
            Assert.assertFalse(hasException);

            User user = userDao.newRandomUser();
            user.setHandle(existingUser.getHandle());
            Inmemo.insertOrUpdate(user);

            List<User> possibleUsers = Arrays.asList(existingUser, user);

            hasException = false;
            try {
                User foundUser = Inmemo.findOnly(false, User.class, new IndexConstraint<>("handle", user.getHandle()));
                Assert.assertNotNull(foundUser);
                Assert.assertTrue(possibleUsers.contains(foundUser));
            } catch (InmemoException e) {
                hasException = e.getMessage().toLowerCase().contains("unique");
            }

            Assert.assertFalse(hasException);

            hasException = false;
            try {
                User foundUser = Inmemo.findOnly(true, User.class, new IndexConstraint<>("handle", user.getHandle()));
                Assert.assertNotNull(foundUser);
                Assert.assertTrue(possibleUsers.contains(foundUser));
            } catch (InmemoException e) {
                hasException = true;
            }

            Assert.assertTrue(hasException);

        }
    }

    @Test
    public void testCompatibleClasses() throws InterruptedException {
        Inmemo.dropTableIfExists(Wrapper.a.class);

        // Test user count.
        {
            Assert.assertEquals(USER_COUNT, userDao.findAll().size());
        }

        // Create table.
        {
            Inmemo.createTable(User.class, "ID", null, new Indices.Builder<User>() {{
                add(Index.create("ID", Long.class, new IndexGetter<User, Long>() {
                    @Override
                    public Long get(final User tableItem) {
                        return tableItem.getId();
                    }
                }));

                add(Index.create("FIRST_HANDLE_LETTER", String.class, new IndexGetter<User, String>() {
                    @Override
                    public String get(final User tableItem) {
                        return tableItem.getHandle().substring(0, 1);
                    }
                }));
            }}.build(), true);
        }

//        Assert.assertEquals(1, Inmemo.findCount(User2.class, new IndexConstraint<Object>("ID", 11), new Matcher<User2>() {
//            @Override
//            public boolean match(User2 tableItem) {
//                return true;
//            }
//        }));

        Assert.assertEquals(1, Inmemo.findCount(Wrapper.a.class, new IndexConstraint<Object>("ID", 11L), new Matcher<Wrapper.a>() {
            @Override
            public boolean match(Wrapper.a tableItem) {
                return true;
            }
        }));

        // Tests that there is no user[id=USER_COUNT + 1], inserts it, waits, tests that the table contains it.
        {
            List<a> users = Inmemo.find(a.class, new IndexConstraint<>("ID", USER_COUNT + 1), new Matcher<a>() {
                @Override
                public boolean match(final a user) {
                    return true;
                }
            });

            if (users.size() != 0) {
                System.out.println(users);
            }

            Assert.assertEquals(0, Inmemo.find(Wrapper.a.class, new IndexConstraint<>("ID", USER_COUNT + 1), new Matcher<Wrapper.a>() {
                @Override
                public boolean match(final Wrapper.a user) {
                    return true;
                }
            }).size());

            userDao.insertRandom();
            Thread.sleep(500);

            Assert.assertEquals(1, Inmemo.find(Wrapper.a.class, new IndexConstraint<>("ID", USER_COUNT + 1), new Matcher<Wrapper.a>() {
                @Override
                public boolean match(final Wrapper.a user) {
                    return true;
                }
            }).size());
        }

        // Tests that there is no user[id=USER_COUNT + 2], inserts it, waits, tests that the table contains it.
        {
            Assert.assertEquals(0, Inmemo.find(Wrapper.a.class, new IndexConstraint<>("ID", USER_COUNT + 2), new Matcher<Wrapper.a>() {
                @Override
                public boolean match(final Wrapper.a user) {
                    return true;
                }
            }).size());

            a x = new a();
            x.setId(USER_COUNT + 2);
            x.setHandle("aa");
            Inmemo.insertOrUpdate(x);

            Assert.assertEquals(1, Inmemo.find(Wrapper.a.class, new IndexConstraint<>("ID", USER_COUNT + 2), new Matcher<Wrapper.a>() {
                @Override
                public boolean match(final Wrapper.a user) {
                    return true;
                }
            }).size());

            Assert.assertEquals(1, Inmemo.find(User.class, new IndexConstraint<>("ID", USER_COUNT + 2), new Matcher<User>() {
                @Override
                public boolean match(final User user) {
                    return user.getPassword() == null;
                }
            }).size());
        }
    }

    @Test
    public void testUnique() throws InterruptedException {
        Inmemo.dropTableIfExists(User.class);

        // Test user count.
        {
            Assert.assertEquals(USER_COUNT, userDao.findAll().size());
        }

        // Create table.
        {
            Inmemo.createTable(User.class, "ID", null, new Indices.Builder<User>() {{
                add(Index.createUnique("ID", Long.class, new IndexGetter<User, Long>() {
                    @Override
                    public Long get(final User user) {
                        return user.getId();
                    }
                }));
                add(Index.createUnique("handle", String.class, new IndexGetter<User, String>() {
                    @Override
                    public String get(final User user) {
                        return user.getHandle();
                    }
                }));
                add(Index.create("FIRST_HANDLE_LETTER", String.class, new IndexGetter<User, String>() {
                    @Override
                    public String get(final User tableItem) {
                        return tableItem.getHandle().substring(0, 1);
                    }
                }));
            }}.build(), true);
        }

        // Assert size.
        {
            Assert.assertEquals(USER_COUNT, Inmemo.size(User.class));
        }

        // Exactly one user with id=123.
        {
            Assert.assertEquals(1, Inmemo.find(User.class, new IndexConstraint<>("ID", 123L), new Matcher<User>() {
                @Override
                public boolean match(final User user) {
                    return true;
                }
            }).size());
        }

        // No one user with id=123 and false matcher.
        {
            Assert.assertEquals(0, Inmemo.find(User.class, new IndexConstraint<>("ID", 123L), new Matcher<User>() {
                @Override
                public boolean match(final User user) {
                    return false;
                }
            }).size());
        }

        // Exactly one user with id=123.
        {
            Assert.assertEquals(1, Inmemo.findCount(User.class, new IndexConstraint<>("ID", 123L)));
        }

        // Exactly one user with id=123.
        {
            Assert.assertEquals(123L, Inmemo.findOnly(true, User.class, new IndexConstraint<>("ID", 123L)).getId());
        }

        // Tests that there is no user[id=USER_COUNT + 1], inserts it, waits, tests that the table contains it.
        {
            Assert.assertNull(Inmemo.findOnly(false, User.class, new IndexConstraint<>("ID", USER_COUNT + 1)));

            userDao.insertRandom();
            Thread.sleep(500);

            Assert.assertEquals(USER_COUNT + 1, Inmemo.findOnly(false, User.class, new IndexConstraint<>("ID", USER_COUNT + 1)).getId());
        }

        // Tests using second index that the number of users with first letter 'e' in handle is expected.
        {
            long id = USER_COUNT / 2;

            User userById = Inmemo.findOnly(true, User.class, new IndexConstraint<>("ID", id));
            Assert.assertEquals(id, userById.getId());

            User userByHandle = Inmemo.findOnly(true, User.class, new IndexConstraint<>("handle", userById.getHandle()));

            Assert.assertNotNull(userByHandle);
            Assert.assertNotSame(userById, userByHandle);
            Assert.assertEquals(userById, userByHandle);

            List<User> usersByFirstHandleLetter = Inmemo.find(User.class, new IndexConstraint<>("FIRST_HANDLE_LETTER", userById.getHandle().substring(0, 1)));
            Assert.assertTrue(usersByFirstHandleLetter.contains(userById));
        }

        // Tests that if non-unique then throw exception.
        {
            User existingUser = Inmemo.findOnly(true, User.class, new IndexConstraint<>("ID", USER_COUNT / 5));
            User user = userDao.newRandomUser();
            user.setHandle(existingUser.getHandle());

            boolean hasException = false;
            try {
                Inmemo.insertOrUpdate(user);
            } catch (InmemoException e) {
                hasException = e.getMessage().toLowerCase().contains("unique");
            }

            Assert.assertTrue(hasException);
        }
    }
}