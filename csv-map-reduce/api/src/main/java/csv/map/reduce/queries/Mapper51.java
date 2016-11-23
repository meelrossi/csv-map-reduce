package csv.map.reduce.queries;

import com.hazelcast.mapreduce.Context;
import com.hazelcast.mapreduce.Mapper;

import csv.map.reduce.api.model.Census;
import csv.map.reduce.api.model.CountyState;

public class Mapper51 implements Mapper<String, Census, CountyState, Integer>{

	private static final long serialVersionUID = 1L;

	public void map(String key, Census value, Context<CountyState, Integer> context) {
		context.emit(new CountyState(value.getCountyName(), value.getStateName()), 1);
	}

}
