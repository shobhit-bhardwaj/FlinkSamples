package com.shobhit.basic;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class WordCountStreamProcessing {
	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
		ParameterTool parameterTool = ParameterTool.fromArgs(args);
		environment.getConfig().setGlobalJobParameters(parameterTool);

		DataStream<String> sourceSet = environment.readTextFile("src\\main\\resources\\word_count.txt");
		System.out.println(sourceSet);

		DataStream<Tuple2<String, Integer>> mapSet = sourceSet
				.flatMap((input, collector) -> {
						String[] tokens = input.split(" ");
						for(String token : tokens)
							collector.collect(new Tuple2<String, Integer>(token, 1));
					})
				.returns(new TupleTypeInfo(TypeInformation.of(String.class), TypeInformation.of(Integer.class)));

		DataStream<Tuple2<String, Integer>> resultSet = mapSet.keyBy(0).sum(1);

		resultSet.print();

		environment.execute();
	}
}