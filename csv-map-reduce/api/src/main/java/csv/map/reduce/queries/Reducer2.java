package csv.map.reduce.queries;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import com.hazelcast.mapreduce.Reducer;
import com.hazelcast.mapreduce.ReducerFactory;

public class Reducer2 implements ReducerFactory<Integer, Integer[], Double> {

	private static final long serialVersionUID = 1L;

	public Reducer<Integer[], Double> newReducer(Integer arg0) {
		return new Reducer<Integer[], Double>() {
			Map<Integer, Integer> map;

			@Override
			public void beginReduce() {
				map = new HashMap<Integer, Integer>();
			}

			@Override
			public Double finalizeReduce() {
				Collection<Integer> values = map.values();
				double sum = 0;
				int count = values.size();
				for (Integer val : values) {
					sum += val;
				}
				return sum / count;
			}

			@Override
			public void reduce(Integer[] cantId) {
				Integer current = map.get(cantId[1]);
				Integer count = current != null ? current + cantId[0] : cantId[0];
				map.put(cantId[1], count);
			}

		};
	}

}
