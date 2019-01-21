package com.neuedu;

//<I,1><have,1><a,1><dream,1><a,1><dream,1>


import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class FiveStepReducer extends Reducer<Text,Text,Text,Text>
{
    Map<String,String> mapGlobalName2Label = new HashMap<>();
    //<一灯大师,#完颜萍:0.004662004662004662;小龙女:0.018648018648018648;尹克西:0.002331002331002331;尼摩星:0.006993006993006993;张无忌:0.004662004662004662;无色:0.002331002331002331;明月:0.002331002331002331;朱九真:0.004662004662004662;朱子柳:0.027972027972027972;朱长龄:0.002331002331002331;李莫愁:0.009324009324009324;杨康:0.006993006993006993;杨过:0.07692307692307693;柯镇恶:0.004662004662004662;梅超风:0.002331002331002331;樵子:0.02097902097902098;欧阳克:0.006993006993006993;武三娘:0.004662004662004662;武三通:0.030303030303030304;武修文:0.006993006993006993;武敦儒:0.004662004662004662;洪七公:0.023310023310023312;渔人:0.02564102564102564;潇湘子:0.002331002331002331;点苍渔隐:0.006993006993006993;王处一:0.004662004662004662;琴儿:0.002331002331002331;穆念慈:0.002331002331002331;老头子:0.002331002331002331;耶律燕:0.006993006993006993;耶律齐:0.011655011655011656;裘千丈:0.002331002331002331;裘千仞:0.030303030303030304;上官:0.002331002331002331;裘千尺:0.013986013986013986;觉远:0.002331002331002331;觉远大师:0.002331002331002331;说不得:0.002331002331002331;达尔巴:0.004662004662004662;郝大通:0.004662004662004662;郭芙:0.009324009324009324;郭襄:0.039627039627039624;郭靖:0.11655011655011654;金轮法王:0.009324009324009324;陆乘风:0.002331002331002331;陆无双:0.016317016317016316;霍都:0.006993006993006993;马钰:0.004662004662004662;鲁有脚:0.009324009324009324;黄药师:0.03263403263403263;黄蓉:0.16783216783216784;小沙弥:0.006993006993006993;丘处机:0.011655011655011656;乔寨主:0.002331002331002331;书生:0.03496503496503497;农夫:0.037296037296037296;华筝:0.002331002331002331;卫璧:0.004662004662004662;吕文德:0.002331002331002331;周伯通:0.06060606060606061;哑巴:0.002331002331002331;哑梢公:0.002331002331002331;大汉:0.002331002331002331;天竺僧:0.002331002331002331;天竺僧人:0.006993006993006993>
//<一灯大师,100#尹克西>
//<一灯大师,250#完颜萍>
//    ...
//<一灯大师,List[100#尹克西,250#完颜萍,#完颜萍:0.004662004662004662;小龙女:0.018648018648018648;尹克西:0.002331002331002331;尼摩星:0.006993006993006993;张无忌:0.004662004662004662;无色:0.002331002331002331;明月:0.002331002331002331;朱九真:0.004662004662004662;朱子柳:0.027972027972027972;朱长龄:0.002331002331002331;李莫愁:0.009324009324009324;杨康:0.006993006993006993;杨过:0.07692307692307693;柯镇恶:0.004662004662004662;梅超风:0.002331002331002331;樵子:0.02097902097902098;欧阳克:0.006993006993006993;武三娘:0.004662004662004662;武三通:0.030303030303030304;武修文:0.006993006993006993;武敦儒:0.004662004662004662;洪七公:0.023310023310023312;渔人:0.02564102564102564;潇湘子:0.002331002331002331;点苍渔隐:0.006993006993006993;王处一:0.004662004662004662;琴儿:0.002331002331002331;穆念慈:0.002331002331002331;老头子:0.002331002331002331;耶律燕:0.006993006993006993;耶律齐:0.011655011655011656;裘千丈:0.002331002331002331;裘千仞:0.030303030303030304;上官:0.002331002331002331;裘千尺:0.013986013986013986;觉远:0.002331002331002331;觉远大师:0.002331002331002331;说不得:0.002331002331002331;达尔巴:0.004662004662004662;郝大通:0.004662004662004662;郭芙:0.009324009324009324;郭襄:0.039627039627039624;郭靖:0.11655011655011654;金轮法王:0.009324009324009324;陆乘风:0.002331002331002331;陆无双:0.016317016317016316;霍都:0.006993006993006993;马钰:0.004662004662004662;鲁有脚:0.009324009324009324;黄药师:0.03263403263403263;黄蓉:0.16783216783216784;小沙弥:0.006993006993006993;丘处机:0.011655011655011656;乔寨主:0.002331002331002331;书生:0.03496503496503497;农夫:0.037296037296037296;华筝:0.002331002331002331;卫璧:0.004662004662004662;吕文德:0.002331002331002331;周伯通:0.06060606060606061;哑巴:0.002331002331002331;哑梢公:0.002331002331002331;大汉:0.002331002331002331;天竺僧:0.002331002331002331;天竺僧人:0.006993006993006993>]
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        Map<String,Double> mapName2Weight = new HashMap<>();
        Map<String,String> mapName2Label = new HashMap<>();


        String peopleRelation = "";
        String peoplePR = "";
        List<String> labelPeopleNameList = new ArrayList<>();
        for(Text text : values)
        {
            String content = text.toString();
            if(content.startsWith("#"))
            {
                peopleRelation = content.replace("#","");
            }
            else if(content.startsWith("@"))
            {
                peoplePR = content.replace("@","");
            }
            else
            {
                //List[100#尹克西,250#完颜萍]
                labelPeopleNameList.add(content);
            }
        }

        String[] arrNameWeight = peopleRelation.split(";");
        for(String string : arrNameWeight)
        {
            //小龙女:0.018648018648018648
            String[] split = string.split(":");
            mapName2Weight.put(split[0],Double.parseDouble(split[1]));
        }

        Map<String,Double> mapLabel2Weight =  new HashMap<>();
        for(String string : labelPeopleNameList)
        {
            //100#尹克西
            String[] split = string.split("#");
            mapName2Label.put(split[1],split[0]);

//            7  zhangsan 0.3
//            7  wangwu   0.4
            String label = split[0];
            if(mapGlobalName2Label.containsKey(split[1]))
            {
                label = mapGlobalName2Label.get(split[1]);
            }

            if(!mapLabel2Weight.containsKey(label))
            {
                mapLabel2Weight.put(label,mapName2Weight.get(split[1]));
            }
            else
            {
                Double weight = mapLabel2Weight.get(label);
                weight += mapName2Weight.get(split[1]);
                mapLabel2Weight.put(label,weight);
            }



        }

        //比较主要人物周围的人，找到关系最密切的那个，将主要人物的标签label更新成那个人的label
        double maxWeight = 0;
//        String peopleMax = "";
        String newLabel = "";
        for(Map.Entry<String,Double> entry : mapLabel2Weight.entrySet())
        {
            double weight = entry.getValue();
            if(maxWeight < weight)
            {
                maxWeight = weight;
                newLabel = entry.getKey();
            }
        }

//        String newLabel = mapName2Label.get(peopleMax);
        mapGlobalName2Label.put(key.toString(),newLabel);

        Text keyWrite = new Text(newLabel + "#" + key.toString());
        Text valueWrite = new Text(peoplePR + "#" + peopleRelation);
        context.write(keyWrite,valueWrite);
    }
}