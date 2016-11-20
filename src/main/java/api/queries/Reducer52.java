package api.queries;

import java.util.LinkedList;
import java.util.List;

import com.hazelcast.mapreduce.Reducer;
import com.hazelcast.mapreduce.ReducerFactory;

import api.model.StatePair;

public class Reducer52 implements ReducerFactory<Integer, String, List<StatePair>> {

	private static final long serialVersionUID = 1L;

	@Override
	public Reducer<String, List<StatePair>> newReducer(Integer key) {
		return new Reducer<String, List<StatePair>>() {
			List<String> states;

			@Override
			public void beginReduce() {
				states = new LinkedList<String>();
			}

			@Override
			public void reduce(String value) {
				states.add(value);
			}

			@Override
			public List<StatePair> finalizeReduce() {
				List<StatePair> pairs = new LinkedList<StatePair>();
				for (int i = 0; i < states.size(); i++) {
					for (int j = i + 1; j < states.size(); j++) {
						pairs.add(new StatePair(states.get(i), states.get(j)));
					}
				}
				return pairs;
			}
		};
	}
}
