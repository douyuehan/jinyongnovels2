package com.neuedu;

//<I,1><have,1><a,1><dream,1><a,1><dream,1>


import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;


public class SecondStepReducer extends Reducer<Text,LongWritable,Text,LongWritable>
{
//    <戚芳_卜垣,list(1,1,1,1)>
    @Override
    protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
        long sum = 0;
        for(LongWritable longWritable : values)
        {
            sum += longWritable.get();
        }

        context.write(key,new LongWritable(sum));

    }
}