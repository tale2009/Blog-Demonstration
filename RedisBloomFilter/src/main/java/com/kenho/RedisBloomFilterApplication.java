package com.kenho;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class RedisBloomFilterApplication {
    public static void main(String args[])
    {
        SpringApplication.run(RedisBloomFilterApplication.class,args);
    }
}
