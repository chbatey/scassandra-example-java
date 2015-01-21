package com.batey.examples.scassandra;

import java.util.Date;
import java.util.List;

public class Person {
    private final String name;
    private final int age;
    private final List<Date> interestingDates;

    public Person(String name, int age, List<Date> interestingDates) {
        this.name = name;
        this.age = age;
        this.interestingDates = interestingDates;
    }

    public String getName() {
        return name;
    }

    public int getAge() {
        return age;
    }

    public List<Date> getInterestingDates() {
        return interestingDates;
    }
}
