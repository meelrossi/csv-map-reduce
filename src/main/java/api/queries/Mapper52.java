package api.queries;

import com.hazelcast.mapreduce.Context;
import com.hazelcast.mapreduce.Mapper;

import api.model.CountyState;

public class Mapper52 implements Mapper<CountyState, Integer, Integer, CountyState> {

	private static final long serialVersionUID = 1L;

	public void map(CountyState key, Integer value, Context<Integer, CountyState> context) {
		context.emit(value, key);
	}

}
