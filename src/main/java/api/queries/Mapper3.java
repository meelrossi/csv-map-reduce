package api.queries;

import com.hazelcast.mapreduce.Context;
import com.hazelcast.mapreduce.Mapper;

import api.model.Census;

public class Mapper3 implements Mapper<String, Census, String, Integer>{

	private static final long serialVersionUID = 1L;

	public void map(String keyinput, Census census, Context<String, Integer> context) {
		context.emit(census.getCountyName(), census.getLiteracy());
	}
}
