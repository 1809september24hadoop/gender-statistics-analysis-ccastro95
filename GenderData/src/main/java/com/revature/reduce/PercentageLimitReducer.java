package com.revature.reduce;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class PercentageLimitReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable>{
	
	@Override
	public void reduce(Text key, Iterable<DoubleWritable> values, Context context) 
			throws IOException, InterruptedException{
		double percentageLimit = 30.00;
		
		for(DoubleWritable value: values) {
			if(value.get() < percentageLimit && value.get() > 0){
				context.write(key, new DoubleWritable(value.get()));
				break;
			}
		}
	}

}
