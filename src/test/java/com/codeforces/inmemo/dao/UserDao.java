package com.codeforces.inmemo.dao;

import com.codeforces.inmemo.model.User;

import java.util.List;

/**
 * @author MikeMirzayanov (mirzayanovmr@gmail.com)
 */
public interface UserDao {
    User find(long id);
    void insertRandom();
    void update(User user);
    void insert(User user);
    List<User> findAll();
    User newRandomUser();

    String getRandomString();
}
