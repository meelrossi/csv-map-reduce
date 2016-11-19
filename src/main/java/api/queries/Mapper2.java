package api.queries;

import com.hazelcast.mapreduce.Context;
import com.hazelcast.mapreduce.Mapper;

import api.model.Census;

public class Mapper2 implements Mapper<String, Census, Integer, Integer[]>{

	private static final long serialVersionUID = 1L;

	public void map(String keyinput, Census census, Context<Integer, Integer[]> context) {
		Integer[] cantId = {1, census.getHouseId()};
		context.emit(census.getHouseType(), cantId);
	}

}
