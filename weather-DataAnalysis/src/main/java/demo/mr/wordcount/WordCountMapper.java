package demo.mr.wordcount;

import java.io.IOException;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;

public class WordCountMapper extends Mapper<LongWritable, Text , Text, Text> {

	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		String line = value.toString();
		String[] words = line.split(" ");
		String id = words[0];
		String cityname = words[1];
		String datetime = StringUtils.substringBeforeLast(words[2],"-");
		String temperture = words[5];
		String sb=StringUtils.substringBefore(temperture, "℃");
		String sb1=StringUtils.substringBefore(words[6], "℃");
    	//int numtemperture =Integer.parseInt(sb);
		context.write(new Text(cityname+"--"+datetime), new Text(sb+"-"+sb1));
		//context.write(new Text(id+"--"+id), new Text(id));
		//context.write(new Text(v), new IntWritable(temperture));
		
		
		/*for(String word: words) {
			context.write(new Text(word), new IntWritable(1));
			
		}*/
		
	}
	
	

}
