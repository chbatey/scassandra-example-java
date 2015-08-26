package com.batey.examples.scassandra;

import java.util.Collections;
import java.util.List;

public class Main {
    public static void main(String[] args) {
        PersonDao dao = new PersonDaoCassandra(9042, 3);

        dao.connect();

        dao.storePerson(new Person("Chris", "Batey", 30, Collections.emptySet()));

        final List<Person> people = dao.retrievePeople();

        people.forEach(System.out::println);

        dao.disconnect();
    }
}
