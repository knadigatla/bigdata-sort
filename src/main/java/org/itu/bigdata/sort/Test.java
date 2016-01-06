package org.itu.bigdata.sort;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.RandomStringUtils;
import org.apache.hadoop.io.Text;

import java.util.ArrayList;

/**
 * Created by kiran on 1/3/16.
 */
public class Test {

    public static void main(String[] args) {
        int length = 10;
        boolean useLetters = true;
        boolean useNumbers = false;
        String generatedString;
        ArrayList<String> alist = new ArrayList<String>();

        for(int i=0; i < 26; i++) {
//            String ttt = RandomStringUtils.random(length, useLetters, useNumbers);
            String ttt = (char)(97+i)+" ";
            System.out.println(ttt+"   "+getPartition(new Text(ttt),null,4) + "    "+(int)ttt.charAt(0));
            System.out.println();



        }

//        System.out.println("the min is "+Math.min(10, 100));
//        System.out.println("the floor is "+Math.floor(10.2));
//        System.out.println("the floor is "+Math.ceil(10.2));
//
//        int t = 6;
//        System.out.println(50.0/t);
//
//        Text tt = new Text("Ziran");
//        int c = tt.charAt(0);
//        System.out.println(c);


//        System.out.println(generatedString);
    }

    public static int getPartition(Text key, Text val, int noOfPartitions) {

        int mid = noOfPartitions/2;
        int incrA = (int)Math.ceil(26.0/mid);
        int incra = (int)Math.ceil(26.0/(noOfPartitions-mid));

        for(int i=0;i<mid;i++) {
            int firstChar = key.charAt(0);
            if(firstChar >= 65+(i*incrA) && firstChar < Math.min((65+((i+1)*incrA)),91)) {
                return i;
            }
        }
        for(int i=mid;i<noOfPartitions;i++) {
            int firstChar = key.charAt(0);
            if(firstChar >= 97+((i-mid)*incra) && firstChar < Math.min((97+((i-mid+1)*incra)),123)) {
                return i;
            }
        }

        return 100;
    }
}
