package com.neuedu;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
//<0,hello kitty>
//<11,hello world>
//
//<hello,1>
//<kitty,1>
//<hello,1>
//<world,1>

/**
 * 该类做为一个 mapTask 使用。类声名中所使用的四个泛型意义为别为：
 *
 * KEYIN:   默认情况下，是mr框架所读到的一行文本的起始偏移量，Long,
 *      但是在hadoop中有自己的更精简的序列化接口，所以不直接用Long，而用LongWritable
 * VALUEIN: 默认情况下，是mr框架所读到的一行文本的内容，String，同上，用Text
 * KEYOUT：  是用户自定义逻辑处理完成之后输出数据中的key，在此处是单词，String，同上，用Text
 * VALUEOUT：是用户自定义逻辑处理完成之后输出数据中的value，在此处是单词次数，Integer，同上，用IntWritable
 */

public class ThirdStepMapper extends Mapper<LongWritable,Text,Text,Text>
{
//hello world

//    齐元凯_吴应熊	1
//    齐元凯_神照上人	1
//    齐元凯_韦小宝	5
//    齐元凯_鳌拜	1
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] arrs = value.toString().split("\t");
        String[] arrs2 = arrs[0].split("_");
        String outValue = arrs2[1] + " " + arrs[1];//吴应熊,1
        context.write(new Text(arrs2[0]), new Text(outValue));
    }




}
