package com.eventprocessing;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
public class DistributedEventProcessingApplication {

    public static void main(String[] args) {
        SpringApplication.run(DistributedEventProcessingApplication.class, args);
    }
}
