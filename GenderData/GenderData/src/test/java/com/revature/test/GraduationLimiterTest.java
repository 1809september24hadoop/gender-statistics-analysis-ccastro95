package com.revature.test;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

import com.revature.map.GraduationMapper;
import com.revature.reduce.PercentageLimitReducer;

public class GraduationLimiterTest {
	private MapDriver<LongWritable, Text, Text, DoubleWritable> mapDriver;
	private ReduceDriver<Text, DoubleWritable, Text, DoubleWritable> reduceDriver;
	private MapReduceDriver<LongWritable, Text, Text, DoubleWritable, Text, DoubleWritable> mapReduceDriver;
	private String testCase = "\"Blah Blah Bobbert\",\"BBB\",\"Gross graduation ratio, tertiary, female (%)\",\"SE.TER.CMPL.FE.ZS\",\"29.729\"";
	
	@Before
	public void setUp() {
		GraduationMapper mapper = new GraduationMapper();
		mapDriver = new MapDriver<>();
		mapDriver.setMapper(mapper);
		
		PercentageLimitReducer reducer = new PercentageLimitReducer();
		reduceDriver = new ReduceDriver<>();
		reduceDriver.setReducer(reducer);
		
		mapReduceDriver = new MapReduceDriver<>();
		mapReduceDriver.setMapper(mapper);
		mapReduceDriver.setReducer(reducer);
	}
	
	@Test
	public void testMapper() {
		mapDriver.withInput(new LongWritable(), new Text(testCase));
		
		mapDriver.withOutput(new Text("Blah Blah Bobbert"), new DoubleWritable(29.729));
		
		mapDriver.runTest();
	}
	
	@Test
	public void testReducer() {
		List<DoubleWritable> values = new ArrayList<DoubleWritable>();
		values.add(new DoubleWritable(13.00));

		reduceDriver.withInput(new Text("Blah Blah Bobbert"), values);
		reduceDriver.withOutput(new Text("Blah Blah Bobbert"), new DoubleWritable(13.00));
		
		reduceDriver.runTest();
	}

	@Test
	public void testMapReduce() {
		mapReduceDriver.withInput(new LongWritable(1), new Text(testCase));
		
		mapReduceDriver.addOutput(new Text("Blah Blah Bobbert"), new DoubleWritable(29.729));
		
		mapReduceDriver.runTest();
	}

}

