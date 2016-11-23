package api.queries;

import com.hazelcast.mapreduce.Combiner;
import com.hazelcast.mapreduce.CombinerFactory;

public class Combiner1 implements CombinerFactory<AgeCategory, Integer, Integer> {

	private static final long serialVersionUID = 1L;

	public Combiner<Integer, Integer> newCombiner(AgeCategory key) {
		return new Combiner<Integer, Integer>() {
			int sum;
			
			@Override
			public void beginCombine() {
				sum = 0;
			}
			
			@Override
			public void combine(Integer value) {
				sum = +value;
			}

			@Override
			public Integer finalizeChunk() {
				return sum;
			}

		};
	}

}
