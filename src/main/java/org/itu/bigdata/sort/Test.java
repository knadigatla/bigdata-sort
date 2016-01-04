package org.itu.bigdata.sort;

import org.apache.commons.lang.RandomStringUtils;

/**
 * Created by kiran on 1/3/16.
 */
public class Test {

    public static void main(String[] args) {
        int length = 10;
        boolean useLetters = true;
        boolean useNumbers = false;
        String generatedString = RandomStringUtils.random(length, useLetters, useNumbers);

        System.out.println(generatedString);
    }
}
