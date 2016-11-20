package api.queries;

import com.hazelcast.mapreduce.Context;
import com.hazelcast.mapreduce.Mapper;

public class Mapper5 implements Mapper<String, Integer, Integer, String> {

	private static final long serialVersionUID = 1L;

	@Override
	public void map(String key, Integer value, Context<Integer, String> context) {
		context.emit(value, key);
	}

}
