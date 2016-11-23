package api.queries;

import java.util.Comparator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.PriorityQueue;

import com.hazelcast.mapreduce.Collator;

public class Collator3 implements Collator<Map.Entry<String, Double>, PriorityQueue<Entry<String, Double>>> {

	public PriorityQueue<Entry<String, Double>> collate(Iterable<Entry<String, Double>> values) {
		PriorityQueue<Entry<String, Double>> answer = new PriorityQueue<Entry<String, Double>>(
				new Comparator<Entry<String, Double>>() {
					public int compare(Entry<String, Double> o1, Entry<String, Double> o2) {
						return (-1) * o1.getValue().compareTo(o2.getValue());
					}

				});
		for (Entry<String, Double> entry : values) {
			answer.offer(entry);
		}
		return answer;
	}
}
