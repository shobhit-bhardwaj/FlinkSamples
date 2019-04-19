package com.shobhit.basic;

import java.util.ArrayList;
import java.util.List;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SplitStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class SplitOperations {
	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
		ParameterTool parameterTool = ParameterTool.fromArgs(args);
		environment.getConfig().setGlobalJobParameters(parameterTool);

		DataStream<String> source = environment.readTextFile("src\\main\\resources\\even_odd_data.txt");

		DataStream<Integer> mapStream = source
				.map(new MapFunction<String, Integer>() {
					@Override
					public Integer map(String input) throws Exception {
						return Integer.parseInt(input);
					}
				});

		SplitStream<Integer> splitStream = mapStream.split(new OutputSelector<Integer>() {
			@Override
			public Iterable<String> select(Integer input) {
				List<String> output = new ArrayList<String>();

				if(input%2 == 0)
					output.add("even");
				else
					output.add("odd");

				return output;
			}
		});

		splitStream.select("even").print();
		splitStream.select("odd").print();

		environment.execute();
	}
}