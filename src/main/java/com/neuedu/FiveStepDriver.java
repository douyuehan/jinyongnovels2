package com.neuedu;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * 相当于一个yarn集群的客户端
 * 需要在此封装我们的mr程序的相关运行参数，指定jar包
 * 最后提交给yarn
 */
public class FiveStepDriver {

    public static void main(String[] args) throws Exception {
        System.setProperty("HADOOP_USER_NAME", "root") ;
        System.setProperty("hadoop.home.dir", "e:/hadoop-2.8.3");
//        if (args == null || args.length == 0) {
//            return;
//        }

//        FileUtil.deleteDir(args[1]);

        for(int i = 14; i < 24; i++)
        {
            Configuration configuration = new Configuration();
            Job job = Job.getInstance(configuration);
            //jar
            job.setJarByClass(FiveStepDriver.class);


            job.setMapperClass(FiveStepMapper.class);
            job.setReducerClass(FiveStepReducer.class);

            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);

            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);

            String inputPath = "output" + i + "/part-r-00000";
            FileInputFormat.setInputPaths(job,new Path(inputPath));
            FileInputFormat.setMaxInputSplitSize(job, 1024*1024);
            String outputPath = "output" + (i + 1);
            FileOutputFormat.setOutputPath(job,new Path(outputPath));

            boolean bResult = job.waitForCompletion(true);
            System.out.println("--------------------------------");
        }


        System.exit(0);

    }
}