package api.queries;

import com.hazelcast.mapreduce.Reducer;
import com.hazelcast.mapreduce.ReducerFactory;

public class Reducer3 implements ReducerFactory<String, Integer[], Double>{

	private static final long serialVersionUID = 1L;

	public Reducer<Integer[], Double> newReducer(String county) {
		return new Reducer<Integer[], Double>() {
			double total;
			double literacy;

			@Override
			public void beginReduce() {
				total = 0;
				literacy = 0;
			}

			@Override
			public void reduce(Integer[] totLit) {
				total += totLit[0];
				literacy += totLit[1];
			}
			
			@Override
			public Double finalizeReduce() {
				return literacy / total;
			}
			
		};
	}

}
