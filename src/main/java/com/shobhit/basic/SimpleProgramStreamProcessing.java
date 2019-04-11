package com.shobhit.basic;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class SimpleProgramStreamProcessing {
	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
		ParameterTool parameterTool = ParameterTool.fromArgs(args);
		environment.getConfig().setGlobalJobParameters(parameterTool);

		DataStream<String> sourceSet = environment.readTextFile("src\\main\\resources\\sample.txt");
		System.out.println(sourceSet);

		DataStream<String> mapSet = sourceSet
				.map(new MapFunction<String, String>() {
					@Override
					public String map(String input) throws Exception {
						return input.toUpperCase();
					}
				});

		mapSet.print();

		environment.execute();
	}
}