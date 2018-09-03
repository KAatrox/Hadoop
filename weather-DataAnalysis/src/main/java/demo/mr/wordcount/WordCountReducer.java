package demo.mr.wordcount;

import java.io.IOException;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import com.sun.tools.javac.util.Convert;

public class WordCountReducer extends Reducer<Text, Text , Text, Text> {

	@Override
	protected void reduce(Text key, Iterable<Text> values,
			Reducer<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException {
		
		Integer sum=0;
		Integer nisum=0;String s=null;
		int avgtemperture=0;
		int niavgtemperture=0;
		int max_day_tmp=0;
		int max_night_tem=0;
		Text t=null;
		int i=0;
		for(Text value :  values) {
			s=value.toString();
			String[] words = s.split("-");
			
			if(i==0){
				max_day_tmp=Integer.parseInt(words[0]);
				max_night_tem=Integer.parseInt(words[1]);
			}else{
				if(max_day_tmp<Integer.parseInt(words[0])){
					max_day_tmp =Integer.parseInt(words[0]);
				}
				if(max_night_tem<Integer.parseInt(words[1])){
					max_night_tem=Integer.parseInt(words[1]);
				}
			}
			sum += Integer.parseInt(words[0]);
			nisum += Integer.parseInt(words[1]);
			i++;
		}
		avgtemperture=sum/i;
		niavgtemperture=nisum/i;
		t =new Text("白天平均温度是"+avgtemperture+"℃        夜间平均温度是"+niavgtemperture+
				"℃         这个月白天的最高温度为"+max_day_tmp+"℃         这个月夜间天的最高温度为"+max_night_tem+"℃");
		context.write(key,new Text(t));
		
		//String val = value.toString();
		//String st[] = val.split("-");

	}


}
