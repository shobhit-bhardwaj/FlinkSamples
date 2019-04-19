package com.shobhit.basic;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class AggregationOperations {
	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
		ParameterTool parameterTool = ParameterTool.fromArgs(args);
		environment.getConfig().setGlobalJobParameters(parameterTool);

		DataStream<String> source = environment.readTextFile("src\\main\\resources\\aggregation_data.txt");

		DataStream<Tuple4<String, String, String, Integer>> mapSet = source
				.flatMap(new FlatMapFunction<String, Tuple4<String, String, String, Integer>>() {
					@Override
					public void flatMap(String input, Collector<Tuple4<String, String, String, Integer>> out)
							throws Exception {
						String[] tokens = input.split(" ");
						out.collect(new Tuple4<String, String, String, Integer>(tokens[1], tokens[2], tokens[3], Integer.parseInt(tokens[4])));
					}
				});

		mapSet.keyBy(0).sum(3).print();
		mapSet.keyBy(0).min(3).print();
		mapSet.keyBy(0).minBy(3).print();
		mapSet.keyBy(0).max(3).print();
		mapSet.keyBy(0).maxBy(3).print();

		environment.execute();
	}
}