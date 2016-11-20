package api.queries;

import com.hazelcast.mapreduce.Reducer;
import com.hazelcast.mapreduce.ReducerFactory;

import api.model.CountyState;

public class Reducer51 implements ReducerFactory<CountyState, Integer, Integer>{

	private static final long serialVersionUID = 1L;

	public Reducer<Integer, Integer> newReducer(CountyState state) {
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
				return sum / 100;
			}
			
		};
	}

}
