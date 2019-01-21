package com.neuedu;


import it.uniroma1.dis.wsngroup.gexf4j.core.*;
import it.uniroma1.dis.wsngroup.gexf4j.core.data.Attribute;
import it.uniroma1.dis.wsngroup.gexf4j.core.data.AttributeClass;
import it.uniroma1.dis.wsngroup.gexf4j.core.data.AttributeList;
import it.uniroma1.dis.wsngroup.gexf4j.core.data.AttributeType;
import it.uniroma1.dis.wsngroup.gexf4j.core.impl.GexfImpl;
import it.uniroma1.dis.wsngroup.gexf4j.core.impl.StaxGraphWriter;
import it.uniroma1.dis.wsngroup.gexf4j.core.impl.data.AttributeListImpl;

import java.io.*;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Map;


public class App5 {

    public static void main(String[] args) throws IOException {
        Gexf gexf = new GexfImpl();
        Calendar date = Calendar.getInstance();

        gexf.getMetadata()
                .setLastModified(date.getTime())
                .setCreator("Gephi.org")
                .setDescription("A Web network");
        gexf.setVisualization(true);

        Graph graph = gexf.getGraph();
        graph.setDefaultEdgeType(EdgeType.UNDIRECTED).setMode(Mode.STATIC);

        AttributeList attrList = new AttributeListImpl(AttributeClass.NODE);
        graph.getAttributeLists().add(attrList);

        Attribute attUrl = attrList.createAttribute("class", AttributeType.INTEGER, "Class");
        Attribute attIndegree = attrList.createAttribute("pageranks", AttributeType.DOUBLE, "PageRank");

        File file = new File("output25/part-r-00000");
        FileReader fileReader = new FileReader(file);
        BufferedReader bufferedReader = new BufferedReader(fileReader);

        String line = bufferedReader.readLine();
        Map<String,Node> name2Node = new HashMap<>();
        //农夫	0.0020768966450448022#丁典:0.5;狄云:0.5
//        Map<农夫_丁典,0.5>
        Map<String,Double> namePair2Weight = new HashMap<>();

        Integer nodeId = 0;
        while(line != null)
        {
            String[] split = line.split("\t");
            String[] split1 = split[1].split("#");
            //农夫	0.0020768966450448022#丁典:0.5;狄云:0.5
            String[] split2 = split1[1].split(";");

            //1#zhangsan
            String[] arr = split[0].split("#");

            for(String string : split2)
            {
                //丁典:0.5
                String[] split3 = string.split(":");
                namePair2Weight.put(arr[1] + "_" + split3[0], Double.parseDouble(split3[1]));
            }

            Node node = graph.createNode(nodeId.toString());
            nodeId++;
            node.setLabel(arr[1])
                    .getAttributeValues()
                    .addValue(attUrl, arr[0])
                    .addValue(attIndegree, split1[0]);

            name2Node.put(arr[1],node);

            line = bufferedReader.readLine();
        }

        int edgeID = 0;
        for(Map.Entry<String,Double> entry : namePair2Weight.entrySet())
        {
            //农夫_丁典
            String namePair = entry.getKey();
            String[] split = namePair.split("_");

            Node node1 = name2Node.get(split[0]);
            Node node2 = name2Node.get(split[1]);
            node1.connectTo(String.valueOf(edgeID++),node2).setWeight(Float.parseFloat(entry.getValue().toString()));
        }



//        Node gephi = graph.createNode("0");
//        gephi
//                .setLabel("郝大通")
//                .getAttributeValues()
//                .addValue(attUrl, "3")
//                .addValue(attIndegree, "0.14658");
//
//
//        Node webatlas = graph.createNode("1");
//        webatlas
//                .setLabel("郝大通")
//                .getAttributeValues()
//                .addValue(attUrl, "3")
//                .addValue(attIndegree, "0.14658");
//
//        gephi.connectTo("0", webatlas).setWeight(0.8f);














        StaxGraphWriter graphWriter = new StaxGraphWriter();
        File f = new File("static_graph_sample.gexf");
        Writer out;
        try {
            out =  new FileWriter(f, false);
            graphWriter.writeToStream(gexf, out, "UTF-8");
            System.out.println(f.getAbsolutePath());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}

