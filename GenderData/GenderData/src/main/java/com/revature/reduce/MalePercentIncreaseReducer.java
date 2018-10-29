package com.revature.reduce;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MalePercentIncreaseReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable>{

	@Override
	public void reduce(Text key, Iterable<DoubleWritable> values, Context context) 
			throws IOException, InterruptedException{
		double nextValue = 0.00;
		double originalValue = 0.00;
		double increaseValue = 0.00;
		double temporaryStorage = 0.00;

		for(DoubleWritable value: values) {
			nextValue = value.get();
			temporaryStorage = nextValue - originalValue;
			if(originalValue != 0.00) {
				increaseValue += temporaryStorage;
			}
			originalValue = value.get();
		}
		context.write(key, new DoubleWritable(increaseValue));
	}
}
