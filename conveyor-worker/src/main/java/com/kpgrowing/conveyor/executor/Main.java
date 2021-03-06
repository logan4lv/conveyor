package com.kpgrowing.conveyor.executor;

import com.kpgrowing.conveyor.common.worker.Worker;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.io.IOException;

@SpringBootApplication
public class Main {

    public static void main(String[] args) throws IOException {
        SpringApplication.run(Main.class, args);

        Worker worker = new Worker();
        worker.start();

        System.in.read();
    }

}
