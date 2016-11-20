package api.model;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
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

import api.queries.AgeCategory;
import api.queries.Mapper1;
import api.queries.Mapper2;
import api.queries.Mapper3;
import api.queries.Mapper4;
import api.queries.Mapper51;
import api.queries.Mapper52;
import api.queries.Reducer1;
import api.queries.Reducer2;
import api.queries.Reducer3;
import api.queries.Reducer4;
import api.queries.Reducer51;
import api.queries.Reducer52;
import client.CensusClient;

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
		logger.info("Inicio trabajo map Reduce");
		try {
			switch (this) {

			case QUERY_1:
				ICompletableFuture<Map<AgeCategory, Integer>> future1 = job.mapper(new Mapper1())
						.reducer(new Reducer1()).submit();

				Set<Entry<AgeCategory, Integer>> ans1 = future1.get().entrySet();

				List<String> lines1 = new LinkedList<String>();
				for (Entry<AgeCategory, Integer> entry : ans1) {
					lines1.add(String.format(QUERY_1_FORMAT, entry.getKey(), entry.getValue()));
				}
				writeFile(lines1);
				break;

			case QUERY_2:

				ICompletableFuture<Map<Integer, Double>> future2 = job.mapper(new Mapper2()).reducer(new Reducer2())
						.submit();
				Set<Entry<Integer, Double>> ans2 = future2.get().entrySet();
				List<String> lines2 = new LinkedList<String>();
				for (Entry<Integer, Double> entry : ans2) {
					lines2.add(String.format(QUERY_2_FORMAT, entry.getKey(), entry.getValue()));
				}
				writeFile(lines2);
				break;

			case QUERY_3:

				ICompletableFuture<Map<String, Double>> future3 = job.mapper(new Mapper3()).reducer(new Reducer3())
						.submit();
				Set<Entry<String, Double>> ans3 = future3.get().entrySet();
				List<String> lines3 = new LinkedList<String>();

				for (Entry<String, Double> entry : ans3) {
					lines3.add(String.format(QUERY_3_FORMAT, entry.getKey(), entry.getValue()));
				}

				writeFile(lines3);
				break;

			case QUERY_4:

				ICompletableFuture<Map<String, Integer>> future4 = job.mapper(new Mapper4()).reducer(new Reducer4())
						.submit();
				Set<Entry<String, Integer>> ans4 = future4.get().entrySet();
				List<String> lines4 = new LinkedList<String>();
				for (Entry<String, Integer> entry : ans4) {
					String tope = System.getProperty("tope");
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
				ICompletableFuture<Map<Integer, List<StatePair>>> future5 = job2.mapper(new Mapper52())
						.reducer(new Reducer52()).submit();
				Set<Entry<Integer, List<StatePair>>> ans5 = future5.get().entrySet();
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
		logger.info("Fin trabajo map Reduce");

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
