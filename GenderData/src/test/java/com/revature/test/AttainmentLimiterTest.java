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

import com.revature.map.EducationAttainmentMapper;
import com.revature.reduce.AttainmentLimitReducer;

public class AttainmentLimiterTest {
	private MapDriver<LongWritable, Text, Text, DoubleWritable> mapDriver;
	private ReduceDriver<Text, DoubleWritable, Text, DoubleWritable> reduceDriver;
	private MapReduceDriver<LongWritable, Text, Text, DoubleWritable, Text, DoubleWritable> mapReduceDriver;
	private String testCase = "\"United States\",\"USA\",\"Educational attainment, completed Bachelor's or equivalent, population 25+ years, female (%)\",\"SE.TER.CMPL.FE.ZS\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\""
			+ "\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"35.85857\",\"37.8298\",\"37.43131\",\"38.22037\",\"\",";

	@Before
	public void setUp() {
		EducationAttainmentMapper mapper = new EducationAttainmentMapper();
		mapDriver = new MapDriver<>();
		mapDriver.setMapper(mapper);

		AttainmentLimitReducer reducer = new AttainmentLimitReducer();
		reduceDriver = new ReduceDriver<>();
		reduceDriver.setReducer(reducer);

		mapReduceDriver = new MapReduceDriver<>();
		mapReduceDriver.setMapper(mapper);
		mapReduceDriver.setReducer(reducer);
	}

	@Test
	public void testMapper() {
		mapDriver.withInput(new LongWritable(), new Text(testCase));

		mapDriver.withOutput(new Text("United States"), new DoubleWritable(38.22037));

		mapDriver.runTest();
	}

	@Test
	public void testReducer() {
		List<DoubleWritable> values = new ArrayList<DoubleWritable>();
		values.add(new DoubleWritable(0.00));
		values.add(new DoubleWritable(34.25));

		reduceDriver.withInput(new Text("United States"), values);
		reduceDriver.withOutput(new Text("United States"), new DoubleWritable(34.25));

		reduceDriver.runTest();
	}

	@Test
	public void testMapReduce() {
		mapReduceDriver.withInput(new LongWritable(1), new Text(testCase));
		mapReduceDriver.withOutput(new Text("United States"), new DoubleWritable(38.22037));

		mapReduceDriver.runTest();
	}
}

