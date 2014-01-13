package com.codeforces.inmemo.model;

import com.codeforces.inmemo.HasId;
import org.jacuzzi.mapping.Id;

import java.util.Date;

/** @author MikeMirzayanov (mirzayanovmr@gmail.com) */
public class User implements HasId {
    @Id
    private long id;
    private String handle;
    private String openId;
    private String email;
    private String password;
    private boolean admin;
    private Date creationTime;
    private Date lastOnlineTime;
    private boolean disabled;

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

    public String getOpenId() {
        return openId;
    }

    public void setOpenId(String openId) {
        this.openId = openId;
    }

    public String getEmail() {
        return email;
    }

    public void setEmail(String email) {
        this.email = email;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public boolean isAdmin() {
        return admin;
    }

    public void setAdmin(boolean admin) {
        this.admin = admin;
    }

    public Date getCreationTime() {
        return creationTime == null ? null : new Date(creationTime.getTime());
    }

    public void setCreationTime(Date creationTime) {
        this.creationTime = creationTime;
    }

    public Date getLastOnlineTime() {
        return lastOnlineTime;
    }

    public void setLastOnlineTime(Date lastOnlineTime) {
        this.lastOnlineTime = lastOnlineTime;
    }

    public boolean isDisabled() {
        return disabled;
    }

    public void setDisabled(boolean disabled) {
        this.disabled = disabled;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        User user = (User) o;

        if (admin != user.admin) return false;
        if (disabled != user.disabled) return false;
        if (id != user.id) return false;
        if (creationTime != null ? !creationTime.equals(user.creationTime) : user.creationTime != null) return false;
        if (email != null ? !email.equals(user.email) : user.email != null) return false;
        if (handle != null ? !handle.equals(user.handle) : user.handle != null) return false;
        if (lastOnlineTime != null ? !lastOnlineTime.equals(user.lastOnlineTime) : user.lastOnlineTime != null)
            return false;
        if (openId != null ? !openId.equals(user.openId) : user.openId != null) return false;
        if (password != null ? !password.equals(user.password) : user.password != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = (int) (id ^ (id >>> 32));
        result = 31 * result + (handle != null ? handle.hashCode() : 0);
        result = 31 * result + (openId != null ? openId.hashCode() : 0);
        result = 31 * result + (email != null ? email.hashCode() : 0);
        result = 31 * result + (password != null ? password.hashCode() : 0);
        result = 31 * result + (admin ? 1 : 0);
        result = 31 * result + (creationTime != null ? creationTime.hashCode() : 0);
        result = 31 * result + (lastOnlineTime != null ? lastOnlineTime.hashCode() : 0);
        result = 31 * result + (disabled ? 1 : 0);
        return result;
    }
}
