package com.batey.examples.scassandra;

import java.util.Date;
import java.util.List;
import java.util.Set;

public class Person {
    private final String firstName;
    private final String lastName;
    private final int age;
    private final Set<Date> interestingDates;

    public Person(String firstName, String lastName, int age, Set<Date> interestingDates) {
        this.firstName = firstName;
        this.lastName = lastName;
        this.age = age;
        this.interestingDates = interestingDates;
    }

    public String getFirstName() {
        return firstName;
    }

    public int getAge() {
        return age;
    }

    public String getLastName() {
        return lastName;
    }

    public Set<Date> getInterestingDates() {
        return interestingDates;
    }

    @Override
    public String toString() {
        return "Person{" +
                "firstName='" + firstName + '\'' +
                ", lastName='" + lastName + '\'' +
                ", age=" + age +
                ", interestingDates=" + interestingDates +
                '}';
    }
}
