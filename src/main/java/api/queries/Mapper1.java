package api.queries;

import com.hazelcast.mapreduce.Context;
import com.hazelcast.mapreduce.Mapper;

import api.model.Census;

public class Mapper1 implements Mapper<String, Census, AgeCategory, Integer>{

	private static final long serialVersionUID = 1L;

	public void map(String keyInput, Census census, Context<AgeCategory, Integer> context) {
		context.emit(AgeCategory.getCategory(census.getAge()), 1);
	}
}
