package csv.map.reduce.queries;

import java.util.Comparator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.PriorityQueue;

import com.hazelcast.mapreduce.Collator;

public class Collator4 implements Collator<Map.Entry<String, Integer>, PriorityQueue<Entry<String, Integer>>> {

	public PriorityQueue<Entry<String, Integer>> collate(Iterable<Entry<String, Integer>> values) {
		PriorityQueue<Entry<String, Integer>> answer = new PriorityQueue<Entry<String, Integer>>(
				new Comparator<Entry<String, Integer>>() {
					public int compare(Entry<String, Integer> o1, Entry<String, Integer> o2) {
						return (-1) * o1.getValue().compareTo(o2.getValue());
					}

				});
		for (Entry<String, Integer> entry : values) {
			answer.offer(entry);
		}
		return answer;
	}
}