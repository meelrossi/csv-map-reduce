package api.queries;

import java.util.Comparator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.PriorityQueue;

import com.hazelcast.mapreduce.Collator;

public class Collator1 implements Collator<Map.Entry<AgeCategory, Integer>, PriorityQueue<Entry<AgeCategory, Integer>>> {

	public PriorityQueue<Entry<AgeCategory, Integer>> collate(Iterable<Map.Entry<AgeCategory, Integer>> values) {
		PriorityQueue<Entry<AgeCategory, Integer>> answer = new PriorityQueue<Entry<AgeCategory, Integer>>(new Comparator<Entry<AgeCategory, Integer>>() {
			public int compare(Entry<AgeCategory, Integer> o1, Entry<AgeCategory, Integer> o2) {
				return o1.getKey().ordinal() - o2.getKey().ordinal();
			}
			
		});
		for (Entry<AgeCategory, Integer> entry : values) {
			answer.offer(entry);
		}
		return answer;
	}

}
