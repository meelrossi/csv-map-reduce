package csv.map.reduce.queries;

import java.util.LinkedList;
import java.util.List;

import com.hazelcast.mapreduce.Reducer;
import com.hazelcast.mapreduce.ReducerFactory;

import csv.map.reduce.api.model.CountyState;
import csv.map.reduce.api.model.StatePair;

public class Reducer52 implements ReducerFactory<Integer, CountyState, List<StatePair>> {

	private static final long serialVersionUID = 1L;

	public Reducer<CountyState, List<StatePair>> newReducer(Integer key) {
		return new Reducer<CountyState, List<StatePair>>() {
			List<CountyState> states;

			@Override
			public void beginReduce() {
				states = new LinkedList<CountyState>();
			}

			@Override
			public void reduce(CountyState value) {
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
