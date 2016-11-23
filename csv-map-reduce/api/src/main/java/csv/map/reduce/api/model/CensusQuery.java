package csv.map.reduce.api.model;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.core.IMap;
import com.hazelcast.mapreduce.Job;
import com.hazelcast.mapreduce.JobTracker;
import com.hazelcast.mapreduce.KeyValueSource;

import csv.map.reduce.queries.AgeCategory;
import csv.map.reduce.queries.Collator1;
import csv.map.reduce.queries.Collator2;
import csv.map.reduce.queries.Collator3;
import csv.map.reduce.queries.Collator4;
import csv.map.reduce.queries.Collator5;
import csv.map.reduce.queries.Combiner1;
import csv.map.reduce.queries.Mapper1;
import csv.map.reduce.queries.Mapper2;
import csv.map.reduce.queries.Mapper3;
import csv.map.reduce.queries.Mapper4;
import csv.map.reduce.queries.Mapper51;
import csv.map.reduce.queries.Mapper52;
import csv.map.reduce.queries.Reducer1;
import csv.map.reduce.queries.Reducer2;
import csv.map.reduce.queries.Reducer3;
import csv.map.reduce.queries.Reducer4;
import csv.map.reduce.queries.Reducer51;
import csv.map.reduce.queries.Reducer52;

public enum CensusQuery {
	QUERY_1, QUERY_2, QUERY_3, QUERY_4, QUERY_5;

	private static final String QUERY_1_FORMAT = "%s = %d";
	private static final String QUERY_2_FORMAT = "%d = %.2f";
	private static final String QUERY_3_FORMAT = "%s = %.2f";
	private static final String QUERY_4_FORMAT = "%s = %d";
	private static final String QUERY_5_FORMAT = "%s (%s) + %s (%s)";
	private static final String STATE_MAP_NAME = "54080-54265-states";

	private static Logger logger = LoggerFactory.getLogger(CensusQuery.class);

	public static CensusQuery getQuery(Integer value) {
		if (value == null) {
			value = 2;
		}
		if (value < 1 || value > 5) {
			System.out.println("Invalid query");
			return null;
		}
		return CensusQuery.values()[value - 1];
	}

	public void run(HazelcastInstance client, IMap<String, Census> myMap) {
		JobTracker tracker = client.getJobTracker("default");

		KeyValueSource<String, Census> source = KeyValueSource.fromMap(myMap);
		Job<String, Census> job = tracker.newJob(source);
		long time = System.currentTimeMillis();
		logger.info("Inicio trabajo map Reduce");
		try {
			switch (this) {

			case QUERY_1:
				ICompletableFuture<PriorityQueue<Entry<AgeCategory, Integer>>> future1 = job.mapper(new Mapper1())
						.combiner(new Combiner1()).reducer(new Reducer1()).submit(new Collator1());
				PriorityQueue<Entry<AgeCategory, Integer>> ans1 = future1.get();
				List<String> lines1 = new LinkedList<String>();
				int size1 = ans1.size();
				for (int i= 0; i < size1; i++) {
					Entry<AgeCategory, Integer> entry = ans1.poll();
					lines1.add(String.format(QUERY_1_FORMAT, entry.getKey(), entry.getValue()));
				}
				writeFile(lines1);
				break;

			case QUERY_2:

				ICompletableFuture<PriorityQueue<Entry<Integer, Double>>> future2 = job.mapper(new Mapper2()).reducer(new Reducer2())
						.submit(new Collator2());
				PriorityQueue<Entry<Integer, Double>> ans2 = future2.get();
				List<String> lines2 = new LinkedList<String>();
				int size = ans2.size();
				for (int i = 0; i < size; i++) {
					Entry<Integer, Double> entry = ans2.poll();
					lines2.add(String.format(QUERY_2_FORMAT, entry.getKey(), entry.getValue()));
				}
				writeFile(lines2);
				break;

			case QUERY_3:

				ICompletableFuture<PriorityQueue<Entry<String, Double>>> future3 = job.mapper(new Mapper3()).reducer(new Reducer3())
						.submit(new Collator3());
				PriorityQueue<Entry<String, Double>> ans3 = future3.get();
				List<String> lines3 = new LinkedList<String>();
				String parN = System.getProperty("n");
				int n = parN == null ? ans3.size() : Integer.parseInt(parN);
				for (int i = 0; i < n; i++) {
					Entry<String, Double> entry = ans3.poll();
					lines3.add(String.format(QUERY_3_FORMAT, entry.getKey(), entry.getValue()));
				}

				writeFile(lines3);
				break;

			case QUERY_4:

				ICompletableFuture<PriorityQueue<Entry<String, Integer>>> future4 = job.mapper(new Mapper4()).reducer(new Reducer4())
						.submit(new Collator4());
				PriorityQueue<Entry<String, Integer>> ans4 = future4.get();
				List<String> lines4 = new LinkedList<String>();
				int size4 = ans4.size();
				for (int i = 0; i < size4; i++) {
					String tope = System.getProperty("tope");
					Entry<String, Integer> entry = ans4.poll();
					if (tope == null || entry.getValue() <= Integer.parseInt(tope)) {
						lines4.add(String.format(QUERY_4_FORMAT, entry.getKey(), entry.getValue()));
					}
				}
				writeFile(lines4);
				break;

			case QUERY_5:

				ICompletableFuture<Map<CountyState, Integer>> auxFuture = job.mapper(new Mapper51())
						.reducer(new Reducer51()).submit();
				Set<Entry<CountyState, Integer>> states = auxFuture.get().entrySet();
				IMap<CountyState, Integer> stateMap = client.getMap(STATE_MAP_NAME);

				for (Entry<CountyState, Integer> entry : states) {
					stateMap.set(entry.getKey(), entry.getValue());
				}

				KeyValueSource<CountyState, Integer> source2 = KeyValueSource.fromMap(stateMap);
				Job<CountyState, Integer> job2 = tracker.newJob(source2);
				ICompletableFuture<PriorityQueue<Entry<Integer, List<StatePair>>>> future5 = job2.mapper(new Mapper52())
						.reducer(new Reducer52()).submit(new Collator5());
				PriorityQueue<Entry<Integer, List<StatePair>>> ans5 = future5.get();
				List<String> lines5 = new LinkedList<String>();
				for (Entry<Integer, List<StatePair>> entry : ans5) {
					if (entry.getValue().size() == 0)
						continue;
					lines5.add(String.format("%d", entry.getKey() * 100));
					for (StatePair pair : entry.getValue()) {
						CountyState countyState1 = pair.getCountyState1();
						CountyState countyState2 = pair.getCountyState2();
						lines5.add(
								String.format(QUERY_5_FORMAT, countyState1.getCountyName(), countyState1.getStateName(),
										countyState2.getCountyName(), countyState2.getStateName()));
					}
				}
				writeFile(lines5);
				break;
			}
		} catch (InterruptedException e) {
			e.printStackTrace();
		} catch (ExecutionException e) {
			e.printStackTrace();
		}
		logger.info(String.format("Fin trabajo map Reduce tiempo de ejecucion: %d", System.currentTimeMillis() - time));

	}

	public void writeFile(List<String> lines) {
		String outPath = System.getProperty("outPath");
		if (outPath == null) {
			outPath = "answer.txt";
		}
		Path file = Paths.get(outPath);
		try {
			Files.write(file, lines, Charset.forName("UTF-8"));
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
