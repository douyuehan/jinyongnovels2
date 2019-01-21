package com.neuedu;


import java.io.*;



public class App3 {

    public static void main(String[] args) throws IOException {


        File file = new File("output13/part-r-00000");
        FileReader fileReader = new FileReader(file);
        BufferedReader bufferedReader = new BufferedReader(fileReader);

        File folder = new File("output14");
        folder.mkdirs();
        File fileWrite = new File("output14/part-r-00000");
        FileWriter fileWriter = new FileWriter(fileWrite);
        BufferedWriter bufferedWriter = new BufferedWriter(fileWriter);


        String line = bufferedReader.readLine();

        Integer index = 1;
        while (line != null) {
            line = index.toString() + "#" + line;
            index++;

            bufferedWriter.write(line);
            bufferedWriter.newLine();

            line = bufferedReader.readLine();
        }

        bufferedReader.close();
        bufferedWriter.close();
        fileReader.close();
        fileWriter.close();
    }

}

