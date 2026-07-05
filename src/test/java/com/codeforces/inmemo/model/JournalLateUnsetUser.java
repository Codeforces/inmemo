package com.codeforces.inmemo.model;

import com.codeforces.inmemo.HasId;
import org.jacuzzi.mapping.Id;

public class JournalLateUnsetUser implements HasId {
    @Id
    private long id;
    private String handle;
    private String email;

    @Override
    public long getId() {
        return id;
    }

    public void setId(long id) {
        this.id = id;
    }

    public String getHandle() {
        return handle;
    }

    public void setHandle(String handle) {
        this.handle = handle;
    }

    public String getEmail() {
        return email;
    }

    public void setEmail(String email) {
        this.email = email;
    }
}
