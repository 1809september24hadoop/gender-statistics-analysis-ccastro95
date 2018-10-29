package com.revature;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.revature.map.MaleEmploymentMapper;
import com.revature.reduce.MalePercentIncreaseReducer;

public class MalePercentageIncrease {

	public static void main(String[] args) throws Exception {
		if(args.length != 2){
			System.out.println("Usage: GenderData <input dir> <output dir>\n");
			System.exit(-1);
		}
		Job job = new Job();

		job.setJarByClass(GraduationLimiter.class);

		job.setJobName("Male Employment Percentage Increase");

		FileInputFormat.setInputPaths(job, new Path(args[0]));

		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.setMapperClass(MaleEmploymentMapper.class);

		job.setReducerClass(MalePercentIncreaseReducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);

		boolean success = job.waitForCompletion(true);
		System.exit(success ? 0 : 1);
	}
	
}
