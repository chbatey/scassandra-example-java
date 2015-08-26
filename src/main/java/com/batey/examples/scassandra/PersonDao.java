package com.batey.examples.scassandra;

import java.util.List;

public interface PersonDao {
    void connect();

    void disconnect();

    List<Person> retrievePeople();

    List<Person> retrievePeopleByName(String firstName, String lastName);

    void storePerson(Person person);
}
