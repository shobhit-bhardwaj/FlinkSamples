package com.shobhit.basic;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.utils.ParameterTool;

public class SimpleProgramBatchProcessing {
	public static void main(String[] args) throws Exception {
		ExecutionEnvironment environment = ExecutionEnvironment.getExecutionEnvironment();
		ParameterTool parameterTool = ParameterTool.fromArgs(args);
		environment.getConfig().setGlobalJobParameters(parameterTool);

		DataSet<String> sourceSet = environment.readTextFile("src\\main\\resources\\sample.txt");
		System.out.println(sourceSet);

		DataSet<String> mapSet = sourceSet
				.map(new MapFunction<String, String>() {
					@Override
					public String map(String input) throws Exception {
						return input.toUpperCase();
					}
				});

		mapSet.print();
	}
}