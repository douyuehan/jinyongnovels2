package com.neuedu;

//<I,1><have,1><a,1><dream,1><a,1><dream,1>


import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;


public class FirstStepReducer extends Reducer<LongWritable,Text,Text,NullWritable>
{

    @Override
    protected void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        Text strNameLine = values.iterator().next();
        if(!strNameLine.toString().equals(""))
        {
            context.write(strNameLine,NullWritable.get());
        }

    }
}