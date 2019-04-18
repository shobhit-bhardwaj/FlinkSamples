package com.shobhit.joins;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;

public class RightJoinFlink {
	public static void main(String[] args) throws Exception {
		ExecutionEnvironment environment = ExecutionEnvironment.getExecutionEnvironment();
		ParameterTool parameterTool = ParameterTool.fromArgs(args);
		environment.getConfig().setGlobalJobParameters(parameterTool);

		DataSet<Tuple2<Integer, String>> personSet = environment.readTextFile("src\\main\\resources\\person_data.txt")
				.map(new MapFunction<String, Tuple2<Integer, String>>() {
					@Override
					public Tuple2<Integer, String> map(String input) throws Exception {
						String[] tokens = input.split(" ");
						return new Tuple2<Integer, String>(Integer.parseInt(tokens[0]), tokens[1]);
					}
				});

		DataSet<Tuple2<Integer, String>> locationSet = environment.readTextFile("src\\main\\resources\\location_data.txt")
				.map(new MapFunction<String, Tuple2<Integer, String>>() {
					@Override
					public Tuple2<Integer, String> map(String input) throws Exception {
						String[] tokens = input.split(" ");
						return new Tuple2<Integer, String>(Integer.parseInt(tokens[0]), tokens[1]);
					}
				});

		DataSet<Tuple3<Integer, String, String>> joinedSet = personSet.rightOuterJoin(locationSet)
				.where(0)
				.equalTo(0)
				.with(new JoinFunction<Tuple2<Integer, String>, Tuple2<Integer, String>, Tuple3<Integer, String, String>>() {
					@Override
					public Tuple3<Integer, String, String> join(Tuple2<Integer, String> first, Tuple2<Integer, String> second) throws Exception {
						String name = first == null ? "NA" : first.f1 ;

						return new Tuple3<Integer, String, String>(second.f0, name, second.f1);
					}
				});

		joinedSet.print();
	}
}