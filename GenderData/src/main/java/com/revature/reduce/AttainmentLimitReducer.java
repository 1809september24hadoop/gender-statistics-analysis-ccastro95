package com.revature.reduce;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class AttainmentLimitReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable>{
	
	@Override
	public void reduce(Text key, Iterable<DoubleWritable> values, Context context) 
			throws IOException, InterruptedException{
		double percentageLimit = 30.00;
		
		for(DoubleWritable value: values) {
			if(value.get() > percentageLimit){
				context.write(key, new DoubleWritable(value.get()));
				break;
			}
		}
	}

}
