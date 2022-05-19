package com.kpgrowing.conveyer.schedule;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.io.IOException;

@SpringBootApplication
public class Main {

    public static void main(String[] args) throws Exception {
        SpringApplication.run(Main.class, args);

        Dispatcher dispatcher = new Dispatcher();
        dispatcher.start();

        System.in.read();
    }

}
