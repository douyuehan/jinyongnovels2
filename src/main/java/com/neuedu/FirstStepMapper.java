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

public class FirstStepMapper extends Mapper<LongWritable,Text,LongWritable,Text>
{
//hello world

//柯镇恶大踏步走到厅中，铁杖在方砖上一落，当的一声，悠悠不绝，嘶哑着嗓子道
    //柯镇恶 梅超风

    Set<String> nameSets = new HashSet<String>();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);

        File file = new File("person/jinyong_all_person.txt");
        FileReader fileReader = new FileReader(file);
        BufferedReader bufferedReader = new BufferedReader(fileReader);
        String line = bufferedReader.readLine();

        while (line != null)
        {
            //process
            DicLibrary.insert(DicLibrary.DEFAULT, line);
            nameSets.add(line);
            line = bufferedReader.readLine();

        }


    }

    //马行空
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        line = line.replace("道","");

        Result result = DicAnalysis.parse(line);
        List<Term> termList=result.getTerms();
        StringBuilder stringBuilder = new StringBuilder();

        for(Term term:termList){
            if(nameSets.contains(term.getName()))
            {
                stringBuilder.append(term.getName() + '\t');
            }
        }

        context.write(key,new Text(stringBuilder.toString()));
    }




}
