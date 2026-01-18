package com.anupambasak;

import com.anupambasak.spring.config.AppConfig;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ComponentScan;

@SpringBootApplication
@ComponentScan(basePackageClasses = AppConfig.class)
public class Main {
    public static void main(String[] args) {
        SpringApplication.run(Main.class, args);
//        final int j = 11;
//        for(int i = 1; i <= 10; i++) {
//            String f = String.format("%3d   X %3d = %3d", j,i,i*j);
//            System.out.println(f);
//            try {
//                Thread.sleep(1000);
//            } catch (InterruptedException e) {
//                throw new RuntimeException(e);
//            }
//        }
    }
}