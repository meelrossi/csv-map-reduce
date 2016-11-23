package api.queries;

import java.util.Comparator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.PriorityQueue;

import com.hazelcast.mapreduce.Collator;

public class Collator2 implements Collator<Map.Entry<Integer, Double>, PriorityQueue<Entry<Integer, Double>>> {

	public PriorityQueue<Entry<Integer, Double>> collate(Iterable<Entry<Integer, Double>> values) {
		PriorityQueue<Entry<Integer, Double>> answer = new PriorityQueue<Entry<Integer, Double>>(
				new Comparator<Entry<Integer, Double>>() {
					public int compare(Entry<Integer, Double> o1, Entry<Integer, Double> o2) {
						return o1.getKey().compareTo(o2.getKey());
					}
				});
		for (Entry<Integer, Double> entry : values) {
			answer.offer(entry);
		}
		return answer;
	}

}
