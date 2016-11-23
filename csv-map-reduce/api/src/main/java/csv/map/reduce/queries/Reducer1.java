package csv.map.reduce.queries;

import com.hazelcast.mapreduce.Reducer;
import com.hazelcast.mapreduce.ReducerFactory;

public class Reducer1 implements ReducerFactory<AgeCategory, Integer, Integer> {

	private static final long serialVersionUID = 1L;

	public Reducer<Integer, Integer> newReducer(AgeCategory category) {
		return new Reducer<Integer, Integer>() {

			private Integer sum;

			@Override
			public void beginReduce() {
				sum = 0;
			}

			@Override
			public void reduce(Integer size) {
				sum += size;
			}

			@Override
			public Integer finalizeReduce() {
				return sum;
			}

		};
	}

}
