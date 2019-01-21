package com.neuedu;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
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

public class FourStepMapper extends Mapper<LongWritable,Text,Text,Text>
{
//凌霜华	0.1#狄云:0.3333333333333333;丁典:0.3333333333333333;戚芳:0.3333333333333333
//凌霜华	#狄云:0.3333333333333333;丁典:0.3333333333333333;戚芳:0.3333333333333333
//<凌霜华,List(xxx,yyy,zzz,#狄云:0.3333333333333333;丁典:0.3333333333333333;戚芳:0.3333333333333333)
//凌霜华	1.26#狄云:0.3333333333333333;丁典:0.3333333333333333;戚芳:0.3333333333333333
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] split = value.toString().split("#");
        String[] split2 = split[0].split("\t");
        String mainPeopleName = split2[0];
        String mainPeopleRank = split2[1];
        String[] split1 = split[1].split(";");
        for(String string : split1)
        {
            String[] split3 = string.split(":");
            double relation_value = Double.parseDouble(split3[1]);
            context.write(new Text(split3[0]),new Text(String.valueOf(relation_value * Double.parseDouble(mainPeopleRank))));
        }

        context.write(new Text(mainPeopleName),new Text("#"+split[1]));
    }




}
