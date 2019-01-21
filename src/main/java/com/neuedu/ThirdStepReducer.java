package com.neuedu;

//<I,1><have,1><a,1><dream,1><a,1><dream,1>


import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;


public class ThirdStepReducer extends Reducer<Text,Text,Text,NullWritable>
{
//<齐元凯,List(吴应熊 1,神照上人 1,韦小宝 5,鳌拜 1)>

//    齐元凯\t吴应熊:0.125;神照上人:0.125;韦小宝:0.625;鳌拜:0.125
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        double sum = 0;
       List<String> textList = new ArrayList<String>();
       for(Text text : values)
       {
           String[] split = text.toString().split(" ");
           long count = Long.parseLong(split[1]);
           sum += count;
           textList.add(text.toString());
       }
        //<齐元凯,List(吴应熊 1,神照上人 1,韦小宝 5,鳌拜 1)>

//    齐元凯\t吴应熊:0.125;神照上人:0.125;韦小宝:0.625;鳌拜:0.125
       StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append(key.toString());
        stringBuilder.append("\t");
        stringBuilder.append("0.1#");
        for(String text : textList)
        {
            String[] split = text.split(" ");
            stringBuilder.append(split[0]);
            stringBuilder.append(":");
            double rate = Long.parseLong(split[1])/sum;
            stringBuilder.append(String.valueOf(rate));
            stringBuilder.append(";");
        }
        String output = stringBuilder.toString();
        output = output.substring(0,output.length()-1);
        context.write(new Text(output),NullWritable.get());
    }
}