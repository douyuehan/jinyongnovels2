package com.neuedu;


import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class App4 {

    public static void main(String[] args) throws IOException {



        File file = new File("output24/part-r-00000");
        FileReader fileReader = new FileReader(file);
        BufferedReader bufferedReader = new BufferedReader(fileReader);

        File folder = new File("output25");
        folder.mkdirs();
        File fileWrite = new File("output25/part-r-00000");
        FileWriter fileWriter = new FileWriter(fileWrite);
        BufferedWriter bufferedWriter = new BufferedWriter(fileWriter);


        String line = bufferedReader.readLine();
        Map<String,List<String>> stringListMap = new HashMap<>();
        Integer index = 1;
        while (line != null) {
//            1095#一灯大师	0.5#完颜萍:0.004662004662004662;小龙女:0.018648018648018648;尹克西:0.002331002331002331;尼摩星:0.006993006993006993;张无忌:0.004662004662004662;无色:0.002331002331002331;明月:0.002331002331002331;朱九真:0.004662004662004662;朱子柳:0.027972027972027972;朱长龄:0.002331002331002331;李莫愁:0.009324009324009324;杨康:0.006993006993006993;杨过:0.07692307692307693;柯镇恶:0.004662004662004662;梅超风:0.002331002331002331;樵子:0.02097902097902098;欧阳克:0.006993006993006993;武三娘:0.004662004662004662;武三通:0.030303030303030304;武修文:0.006993006993006993;武敦儒:0.004662004662004662;洪七公:0.023310023310023312;渔人:0.02564102564102564;潇湘子:0.002331002331002331;点苍渔隐:0.006993006993006993;王处一:0.004662004662004662;琴儿:0.002331002331002331;穆念慈:0.002331002331002331;老头子:0.002331002331002331;耶律燕:0.006993006993006993;耶律齐:0.011655011655011656;裘千丈:0.002331002331002331;裘千仞:0.030303030303030304;上官:0.002331002331002331;裘千尺:0.013986013986013986;觉远:0.002331002331002331;觉远大师:0.002331002331002331;说不得:0.002331002331002331;达尔巴:0.004662004662004662;郝大通:0.004662004662004662;郭芙:0.009324009324009324;郭襄:0.039627039627039624;郭靖:0.11655011655011654;金轮法王:0.009324009324009324;陆乘风:0.002331002331002331;陆无双:0.016317016317016316;霍都:0.006993006993006993;马钰:0.004662004662004662;鲁有脚:0.009324009324009324;黄药师:0.03263403263403263;黄蓉:0.16783216783216784;小沙弥:0.006993006993006993;丘处机:0.011655011655011656;乔寨主:0.002331002331002331;书生:0.03496503496503497;农夫:0.037296037296037296;华筝:0.002331002331002331;卫璧:0.004662004662004662;吕文德:0.002331002331002331;周伯通:0.06060606060606061;哑巴:0.002331002331002331;哑梢公:0.002331002331002331;大汉:0.002331002331002331;天竺僧:0.002331002331002331;天竺僧人:0.006993006993006993
            String[] arr = line.split("#");
            if(!stringListMap.containsKey(arr[0]))
            {
                List<String> list = new ArrayList<>();
                list.add(arr[1] + "#" + arr[2]);
                stringListMap.put(arr[0],list);
            }
            else
            {
                List<String> list = stringListMap.get(arr[0]);
                list.add(arr[1] + "#" + arr[2]);
            }

            line = bufferedReader.readLine();
        }
        Integer indexCount = 1;
        for(Map.Entry<String,List<String>> entry: stringListMap.entrySet())
        {
            List<String> list = entry.getValue();

            for(String string : list)
            {
                bufferedWriter.write(indexCount + "#" + string);
                bufferedWriter.newLine();
            }
            indexCount++;
        }

        bufferedReader.close();
        bufferedWriter.close();
        fileReader.close();
        fileWriter.close();



    }

}

