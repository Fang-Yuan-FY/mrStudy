package com.hypers.mr;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/*
Reducer 对应的四个泛型解释
    KEYIN       : K2的类型
    VALUEIN     : V2的类型

    KEYOUT      : K3的类型
    VALUEOUT    : V3的类型
 */

/**
 *
 */
public class WordCountReduce extends Reducer<Text , LongWritable , Text , LongWritable> {
    //reduce 方法作用 ： 把新的K2 和 V2 转换成 K3 和 V3  将 K3 和 V３写入上下文中
    /*
        参数：
        key     :    新K2
        value   :    是一个集合，存放着新的 V2
        context :    表示上下文对象
        -------------------------
        怎么转换成K3 V3
        K2 可以不变
        V2 遍历集合相加
     */
    @Override
    protected void reduce(Text key, Iterable<LongWritable> values, Reducer<Text, LongWritable, Text, LongWritable>.Context context) throws IOException, InterruptedException {
        //遍历集合，将集合中的数字相加，得到V3
        long count = 0 ;
        for (LongWritable value : values) {
            count += value.get() ; //因为这得到的count 是一个long类型所以需要调用.get()方法来转换成LongWritable
        }
        //将K3 和 V3 写入上下文中
        context.write(key , new LongWritable(count));
    }
}


























