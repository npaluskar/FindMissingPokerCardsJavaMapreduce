/*
 *  Author : Nachiket Paluskar
 *  Class : Mapper class
 *  Description : reading from text file and setting key as first column and value as second column
 *  */

import java.io.IOException;
import java.util.StringTokenizer;
 
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


class MissingCardsMapper extends Mapper<LongWritable, Text, Text, IntWritable>
{
	Text textKey = new Text();
	Text textValue = new Text();
  
  public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
  {
	  String line = value.toString();
	  String[] field = line.split("\t");
	
	  textKey.set(field[0]);
      	  textValue.set(field[1]);
      
	  context.write(textKey,new IntWritable(Integer.parseInt(field[1])));
  }
}
