package com.codeforces.inmemo.model;

import com.codeforces.inmemo.HasId;
import org.jacuzzi.mapping.Id;

/** @author MikeMirzayanov (mirzayanovmr@gmail.com) */
public class Person implements HasId {
    @Id
    private long id;
    private Sex sex;

    @Override
    public long getId() {
        return id;
    }

    public void setId(long id) {
        this.id = id;
    }

    public Sex getSex() {
        return sex;
    }

    public void setSex(Sex sex) {
        this.sex = sex;
    }

    public enum  Sex {
        MALE,
        FEMALE
    }
}
