package com.shobhit.basic;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.aggregation.Aggregations;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.api.java.utils.ParameterTool;

public class WordCountBatchProcessing {
	public static void main(String[] args) throws Exception {
		ExecutionEnvironment environment = ExecutionEnvironment.getExecutionEnvironment();
		ParameterTool parameterTool = ParameterTool.fromArgs(args);
		environment.getConfig().setGlobalJobParameters(parameterTool);

		DataSet<String> sourceSet = environment.readTextFile("src\\main\\resources\\word_count.txt");
		System.out.println(sourceSet);

		DataSet<Tuple2<String, Integer>> mapSet = sourceSet
				.flatMap((input, collector) -> {
						String[] tokens = input.split(" ");
						for(String token : tokens)
							collector.collect(new Tuple2<String, Integer>(token, 1));
					})
				.returns(new TupleTypeInfo(TypeInformation.of(String.class), TypeInformation.of(Integer.class)));

		DataSet<Tuple2<String, Integer>> resultSet = mapSet.groupBy(0).aggregate(Aggregations.SUM, 1);

		resultSet.print();
	}
}