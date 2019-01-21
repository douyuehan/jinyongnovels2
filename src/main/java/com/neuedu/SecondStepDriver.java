package com.neuedu;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * 相当于一个yarn集群的客户端
 * 需要在此封装我们的mr程序的相关运行参数，指定jar包
 * 最后提交给yarn
 */
public class SecondStepDriver {

    public static void main(String[] args) throws Exception {
        System.setProperty("HADOOP_USER_NAME", "root") ;
        System.setProperty("hadoop.home.dir", "e:/hadoop-2.8.3");
        if (args == null || args.length == 0) {
            return;
        }

        FileUtil.deleteDir(args[1]);


        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration);
        //jar
        job.setJarByClass(SecondStepDriver.class);


        job.setMapperClass(SecondStepMapper.class);
        job.setReducerClass(SecondStepReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        FileInputFormat.setInputPaths(job,new Path(args[0]));
        FileInputFormat.setMaxInputSplitSize(job, 1024*1024);
        FileOutputFormat.setOutputPath(job,new Path(args[1]));

        boolean bResult = job.waitForCompletion(true);
        System.out.println("--------------------------------");
        System.exit(bResult ? 0 : 1);

    }
}