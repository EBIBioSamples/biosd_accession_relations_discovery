package com.example;

/**
 * Created by lucacherubin on 2016/09/26.
 */
public class Checker {
    String value;
    long time;

    static Checker withValues(String string, long time) {
        Checker newChecker = new Checker();
        newChecker.value = string;
        newChecker.time = time;
        return newChecker;
    }
}
