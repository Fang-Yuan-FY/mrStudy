package com.hypers.wordcount;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
Mapper 对应的四个泛型解释
    KEYIN       : K1的类型
    VALUEIN     : V1的类型

    KEYOUT      : K2的类型
    VALUEOUT    : V2的类型
 */

public class WordCountMapper extends Mapper<LongWritable , Text , Text , LongWritable> {
    //map方法就是把 K1 V1 转化成 K2 V2
    /**
    参数：
        key     : K1  行偏移量
        value   : V1  每一行对应的文本数据
        context : 表示上下文内容
     */
    /**
    把K1 V1 转化成 K2 V2
        K1      V1
        0   hello， world , hadoop
        15  hive , hello
        -------------------------
        K2      V2
        hive     1
        hello    1
        hello    1
        world    1
        hadoop   1
     */
    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, LongWritable>.Context context) throws IOException, InterruptedException {
        //先把每一行的文本数据进行拆分

        Text text = new Text();
        LongWritable longWritable = new LongWritable();

        String[] split = value.toString().split(",");

        //遍历数组，开始组装K2 V2
        for (String word: split) {
            //把 K2 V2 写入
            //context.write(new Text(word) , new LongWritable(1));
            //方法抽出
            text.set(word);
            longWritable.set(1);
            context.write(text , longWritable);
        }
    }
}




























