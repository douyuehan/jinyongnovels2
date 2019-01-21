package com.neuedu;

import org.ansj.domain.Result;
import org.ansj.domain.Term;
import org.ansj.library.DicLibrary;
import org.ansj.splitWord.analysis.DicAnalysis;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashSet;
import java.util.List;
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

public class SecondStepMapper extends Mapper<LongWritable,Text,Text,LongWritable>
{
//hello world
//刘一舟	韦小宝	刘一舟	韦小宝	刘一舟	韦小宝	韦小宝


    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] arrs = value.toString().split("\t");
        Set<String> nameSets = new HashSet<String>();
        for(String name : arrs)
        {
            nameSets.add(name);
        }

        for(String name1 : nameSets) {
            for (String name2 : nameSets) {
                if (!name1.equals(name2)) {
                    String outputKey = name1 + "_" + name2;
                    context.write(new Text(outputKey), new LongWritable(1));
                }
            }
        }
    }




}
