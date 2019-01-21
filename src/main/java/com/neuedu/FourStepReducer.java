package com.neuedu;

//<I,1><have,1><a,1><dream,1><a,1><dream,1>


import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;


public class FourStepReducer extends Reducer<Text,Text,Text,Text>
{
    //<凌霜华,List(xxx,yyy,zzz,#狄云:0.3333333333333333;丁典:0.3333333333333333;戚芳:0.3333333333333333)
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        double sum = 0;
        String tail = "";
//        凌霜华	0.1#狄云:0.3333333333333333;丁典:0.3333333333333333;戚芳:0.3333333333333333
        for(Text text : values)
        {
            if(!text.toString().startsWith("#"))
            {
                sum+=Double.parseDouble(text.toString());
            }
            else
            {
                tail = text.toString();
            }
        }
        context.write(key,new Text(sum + tail));


    }
}