package com.kenho.util;


import net.openhft.hashing.LongHashFunction;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.data.redis.serializer.StringRedisSerializer;


import java.util.Arrays;


public class RedisBloomFilter {
    //预估插入数量
    private int forecastNumber;
    //可承受误差
    private float bearError;
    //位图长度
    private long bitslength;
    //hash方法个数
    private int hashFunctionsNum;

    private RedisTemplate redisTemplate;
    //setbit lua 语句
    private final String setString="local actorlist = ARGV "+
            "for i,v in pairs(actorlist) do" +
            "    redis.call('setbit',KEYS[1],tonumber(v,10),1) "+
            "end";
    //getbit lua 语句
    private final String getString="local actorlist = ARGV "+
            "for i,v in pairs(actorlist) do" +
            "    if(redis.call('getbit',KEYS[1],tonumber(v,10))==0)  " +
            "    then" +
            "       return 0" +
            "    end "+
            "end"+
            " return 1";
    private static DefaultRedisScript<Long> setScript;
    private static DefaultRedisScript<Long> getScript;
    /**
     * bitslength&hashFunctionsNum参考文章分析使用的是
     * http://en.wikipedia.org/wiki/Bloom_filter#Probability_of_false_positives for the formula 提供的运算逻辑
     * @param forecastNumber 预估数量插入量
     * @param bearError 可接受误差
     */
    private RedisBloomFilter(int forecastNumber,float bearError,RedisTemplate redisTemplate)
    {
        this.redisTemplate=redisTemplate;
        this.forecastNumber=forecastNumber;
        this.bearError=bearError;
        this.bitslength = (long) (-forecastNumber * Math.log(bearError) / (Math.log(2) * Math.log(2)));
        this.hashFunctionsNum=(int) Math.round((double) this.bitslength / this.forecastNumber * Math.log(2));
        this.setScript = new DefaultRedisScript<>(setString, Long.class);
        this.getScript = new DefaultRedisScript<>(getString, Long.class);
    }

    public static  RedisBloomFilter create(int forecastNumber,float bearError,RedisTemplate redisTemplate){
        RedisBloomFilter redisBloomFilter=new RedisBloomFilter(forecastNumber,bearError,redisTemplate);
        return redisBloomFilter;
    }


    public  Boolean put(String key,String str)
    {
        String[] bloomresultlist = toBitList(str);
        setbits(bloomresultlist,key);
        return true;
    }

    public  Boolean checkexist(String key,String str)
    {
        String[] bloomresultlist = toBitList(str);
        return getbits(bloomresultlist,key);
    }

    /**
     * 参考Guava的布隆过滤器设计
     * 基于Less Hashing, Same Performance: Building a Better Bloom Filter论文概念实现
     * @param str
     * @return
     */
    private String[]  toBitList(String str)
    {
        String[] bloomresultlist = new String[hashFunctionsNum];
        Long hash1=LongHashFunction.murmur_3().hashChars(str);
        Long hash2=LongHashFunction.city_1_1().hashChars(str);
        for(int i=0;i<hashFunctionsNum;i++)
        {
            long bloomresult=hash1+i*hash2+i*i;
            //确保结果在申请的位置中
            bloomresultlist[i]=String.valueOf((bloomresult & Long.MAX_VALUE) % bitslength);
        }
        return bloomresultlist;
    }
    private Boolean setbits(String[] bloomresultlist,String redisKey)
    {
        redisTemplate.execute(setScript, new StringRedisSerializer(), new StringRedisSerializer(), Arrays.asList(redisKey),bloomresultlist);
        return true;
    }
    private Boolean getbits(String[] bloomresultlist,String redisKey)
    {
        if((Long)redisTemplate.execute(getScript, new StringRedisSerializer(), new StringRedisSerializer(), Arrays.asList(redisKey), bloomresultlist)!=1)
            return false;
        else
            return true;
    }
}