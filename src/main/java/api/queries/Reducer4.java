package api.queries;

import com.hazelcast.mapreduce.Reducer;
import com.hazelcast.mapreduce.ReducerFactory;

public class Reducer4 implements ReducerFactory<String, Integer, Integer>{

	private static final long serialVersionUID = 1L;

	public Reducer<Integer, Integer> newReducer(String arg0) {
		return new Reducer<Integer, Integer>() {
			int sum;
			
			@Override
			public void beginReduce() {
				sum = 0;
			}

			@Override
			public void reduce(Integer value) {
				sum += value;
			}

			@Override
			public Integer finalizeReduce() {
				return sum;
			}
			
		};
	}

}
