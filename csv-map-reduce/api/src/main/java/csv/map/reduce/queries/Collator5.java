package csv.map.reduce.queries;

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.PriorityQueue;

import com.hazelcast.mapreduce.Collator;

import csv.map.reduce.api.model.StatePair;

public class Collator5 implements Collator<Map.Entry<Integer, List<StatePair>>, PriorityQueue<Entry<Integer, List<StatePair>>>> {

	public PriorityQueue<Entry<Integer, List<StatePair>>> collate(Iterable<Entry<Integer, List<StatePair>>> values) {
		PriorityQueue<Entry<Integer, List<StatePair>>> answer = new PriorityQueue<Entry<Integer, List<StatePair>>>(
				new Comparator<Entry<Integer, List<StatePair>>>() {
					public int compare(Entry<Integer, List<StatePair>> o1, Entry<Integer, List<StatePair>> o2) {
						return o1.getKey().compareTo(o2.getKey());
					}

				});
		for (Entry<Integer, List<StatePair>> entry : values) {
			answer.offer(entry);
		}
		return answer;
	}
}