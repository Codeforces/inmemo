package com.codeforces.inmemo;

/**
 * @author Mike Mirzayanov (mirzayanovmr@gmail.com)
 */

public class Person {
    int a;
    Integer aa;
    double b;
    A ea;

    public Person() {
    }

    public Person(int a, Integer aa, double b, A ea) {
        this.a = a;
        this.aa = aa;
        this.b = b;
        this.ea = ea;
    }

    public int getA() {
        return a;
    }

    public void setA(int a) {
        this.a = a;
    }

    public Integer getAa() {
        return aa;
    }

    public void setAa(Integer aa) {
        this.aa = aa;
    }

    public double getB() {
        return b;
    }

    public void setB(double b) {
        this.b = b;
    }

    public A getEa() {
        return ea;
    }

    public void setEa(A ea) {
        this.ea = ea;
    }

    public enum A {
        X, Y, Z
    }
}
