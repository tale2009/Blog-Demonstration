package com.kenho.controller;

import com.kenho.util.RedisBloomFilter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.client.RestTemplate;

@RestController
public class RedisBloomFilterController {
    @Autowired
    private RedisTemplate redisTemplate;

    @RequestMapping("test")
    public void test(String name)
    {
        RedisBloomFilter redisBloomFilter=RedisBloomFilter.create(10000000, (float) 0.01,redisTemplate);
        redisBloomFilter.put("test",name);
        System.out.println(redisBloomFilter.checkexist("test",name));
    }
}
