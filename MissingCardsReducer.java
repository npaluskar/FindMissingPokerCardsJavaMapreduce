/*
 *  * Author : Nachiket Paluskar
 *  Class : Reducer class
 *  Description : find missing card from a list of deck
 *  */

import java.io.IOException;
import java.util.ArrayList; 
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;

class MissingCardsReducer extends Reducer<Text, IntWritable, Text, Text>
{
Text missing = new Text();

public void reduce(Text token, Iterable<IntWritable> counts, Context context)
        throws IOException, InterruptedException {

ArrayList<Integer> nums = new ArrayList<Integer>();

int sum=0;
int temp=0;

for (IntWritable val : counts) {
	  sum+= val.get();
	  temp=val.get();
	  nums.add(temp);
}

StringBuilder sb = new StringBuilder();

if(sum<91){
	for (int i=1;i<=13;i++){
		if(!nums.contains(i))
			sb.append(i).append(",");
	}
	missing.set(sb.substring(0,sb.length()-1));
}
else{
	 missing.set("No Missing Card here");
	}
context.write(token, missing);
    }
}
