package com.hypers.mr;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class JobMain extends Configured implements Tool {
    //该方法用于指定一个job任务
    @Override
    public int run(String[] strings) throws Exception {
        //job 任务流程
        //1、创建一个JOB任务对象
        Job job = Job.getInstance(super.getConf(), "wordcount");

        //2、配置job任务对象（八大步）

        //1、指定文件的读取方式和读取路径
        job.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.addInputPath(job, new Path("D:\\2021Core\\mrStudy\\src\\main\\java\\mrStudy.txt"));

        //2、指定Map阶段的处理方式和数据类型
        job.setMapperClass(WordCountMapper.class);
        //设置Map阶段K2的类型
        job.setMapOutputKeyClass(Text.class);
        //设置Map阶段V2的类型
        job.setMapOutputValueClass(LongWritable.class);

        // 3 4 5 6 步 即shuffer阶段 采用默认

        //7、指定Reduce阶段的处理方式和数据类型
        job.setReducerClass(WordCountReduce.class);
        //设置K3 的类型
        job.setOutputKeyClass(Text.class);
        //设置V3 的类型
        job.setMapOutputValueClass(LongWritable.class);

        //8、设置输出类型
        job.setOutputFormatClass(TextOutputFormat.class);
        //设置输出的路径
        TextOutputFormat.setOutputPath(job, new Path("D:\\2021Core\\mrStudy\\src\\main\\java\\com\\hypers\\mr\\kycg"));

        //等待任务结束
        boolean b1 = job.waitForCompletion(true);

        return b1 ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        //这建的这个Configuration对象实际上传给了Configured的父类也就是Job Main的父类
        Configuration configuration = new Configuration();
        //启动job任务
        int run = ToolRunner.run(configuration, new JobMain(), args);
        System.exit(run);
    }
}
