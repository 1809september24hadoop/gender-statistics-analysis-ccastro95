package com.revature.map;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MaleEmploymentMapper extends Mapper<LongWritable, Text, Text, DoubleWritable>{

	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException{

		String records = value.toString().substring(1, value.toString().length());

		String[] parts = records.split("\",\"");

		double lastValue = 0.00;

		if(parts[2].equals("Employment to population ratio, 15+, male (%) (national estimate)")) {
			for(int i = 44; i < parts.length; i++){
				try {
					if(i == 44) {
						context.write(new Text(parts[0]), new DoubleWritable(Double.parseDouble(parts[i])));
					}
					lastValue = Double.parseDouble(parts[i]);
				} catch (NumberFormatException e){
					continue;
				}
			}
		}
		context.write(new Text(parts[0]), new DoubleWritable(lastValue));
	}
}
