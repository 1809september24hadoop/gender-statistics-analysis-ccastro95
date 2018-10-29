package com.revature.map;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class EducationAttainmentMapper extends Mapper<LongWritable, Text, Text, DoubleWritable>{

	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException{
		
		String records = value.toString().substring(1, value.toString().length());

		String[] parts = records.split("\",\"");
		
		if(parts[2].equals("Educational attainment, completed Bachelor's or equivalent, population 25+ years, female (%)")) {
			for(int i = parts.length - 1; i > 3; i--){
				try {
					context.write(new Text(parts[0]), new DoubleWritable(Double.parseDouble(parts[i])));
					break;
				} catch (NumberFormatException e){
					continue;
				}
			}
		}
	}
}
