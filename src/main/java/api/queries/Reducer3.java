package api.queries;

import com.hazelcast.mapreduce.Reducer;
import com.hazelcast.mapreduce.ReducerFactory;

public class Reducer3 implements ReducerFactory<String, Integer, Double> {

	private static final long serialVersionUID = 1L;

	public Reducer<Integer, Double> newReducer(String county) {
		return new Reducer<Integer, Double>() {
			double total;
			double literacy;

			@Override
			public void beginReduce() {
				total = 0;
				literacy = 0;
			}

			@Override
			public void reduce(Integer l) {
				if (l == 2)
					literacy++;
				if (l != 0)
					total ++;
			}

			@Override
			public Double finalizeReduce() {
				System.out.println("LE" + literacy);
				System.out.println("TPTA:" + total);
				return literacy / total;
			}

		};
	}

}
