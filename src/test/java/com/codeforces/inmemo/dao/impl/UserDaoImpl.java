package com.codeforces.inmemo.dao.impl;

import com.codeforces.inmemo.dao.UserDao;
import com.codeforces.inmemo.model.User;
import com.codeforces.inmemo.model.User2;
import net.sf.cglib.beans.BeanCopier;
import org.jacuzzi.core.GenericDaoImpl;
import org.jacuzzi.core.Row;
import org.jacuzzi.core.TypeOracle;

import javax.sql.DataSource;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Random;

/**
 * @author MikeMirzayanov (mirzayanovmr@gmail.com)
 */
public class UserDaoImpl extends GenericDaoImpl<User, Long> implements UserDao {
    private Random random = new Random();

    public UserDaoImpl(DataSource dataSource) {
        super(dataSource);
    }

    @Override
    public User find(long id) {
        return super.find(id);
    }

    @Override
    public void insertRandom() {
        User user = new User();
        user.setHandle(getRandomString());
        user.setEmail(getRandomString() + "@gmail.com");
        user.setOpenId(getRandomString() + "@openid");
        user.setPassword("pw" + getRandomString());
        user.setCreationTime(new Date(new Date().getTime() + random.nextInt(1000000)));
        user.setLastOnlineTime(new Date(new Date().getTime() + random.nextInt(1000000)));
        user.setDisabled(random.nextBoolean());
        user.setAdmin(random.nextBoolean());
        insert(user);
    }

    private String getRandomString() {
        StringBuilder result = new StringBuilder();

        for (int i = 0; i < 16; i++) {
            result.append((char)('a' + random.nextInt(26)));
        }

        return result.toString();
    }
}
