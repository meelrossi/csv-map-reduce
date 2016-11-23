package csv.map.reduce.queries;

import com.hazelcast.mapreduce.Context;
import com.hazelcast.mapreduce.Mapper;

import csv.map.reduce.api.model.Census;

public class Mapper4 implements Mapper<String, Census, String, Integer> {

	private static final long serialVersionUID = 1L;

	public void map(String county, Census census, Context<String, Integer> context) {
		context.emit(census.getCountyName(), 1);
	}

}
