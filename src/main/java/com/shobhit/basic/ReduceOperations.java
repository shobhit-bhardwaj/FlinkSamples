package com.shobhit.basic;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class ReduceOperations {
	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
		ParameterTool parameterTool = ParameterTool.fromArgs(args);
		environment.getConfig().setGlobalJobParameters(parameterTool);

		DataStream<String> source = environment.readTextFile("src\\main\\resources\\sales_data.txt");

		DataStream<Tuple5<String, String, String, Integer, Integer>> mapSet = source
				.flatMap(new FlatMapFunction<String, Tuple5<String, String, String, Integer, Integer>>() {
					@Override
					public void flatMap(String input, Collector<Tuple5<String, String, String, Integer, Integer>> out) throws Exception {
						String[] tokens = input.split(" ");
						out.collect(new Tuple5<String, String, String, Integer, Integer>(tokens[1], tokens[2], tokens[3], Integer.parseInt(tokens[4]), 1));
					}
				});

		mapSet
			.keyBy(0)
			.reduce(new ReduceFunction<Tuple5<String,String,String,Integer,Integer>>() {
				@Override
				public Tuple5<String, String, String, Integer, Integer> reduce(
						Tuple5<String, String, String, Integer, Integer> value1,
						Tuple5<String, String, String, Integer, Integer> value2) throws Exception {
					return new Tuple5<String, String, String, Integer, Integer>(value1.f0, value1.f1, value1.f2, value1.f3 + value2.f3, value1.f4 + value2.f4);
				}
			});

		DataStream<Tuple2<String, Double>> resultSet = mapSet
				.map(new MapFunction<Tuple5<String,String,String,Integer,Integer>, Tuple2<String, Double>>() {
					@Override
					public Tuple2<String, Double> map(Tuple5<String, String, String, Integer, Integer> value)
							throws Exception {
						// TODO Auto-generated method stub
						return new Tuple2<String, Double>(value.f0, new Double(value.f3*1.0 / value.f4));
					}
				});

		resultSet.print();

		environment.execute();
	}
}