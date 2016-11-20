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
import api.queries.Mapper5;
import api.queries.Reducer1;
import api.queries.Reducer2;
import api.queries.Reducer3;
import api.queries.Reducer4;
import api.queries.Reducer51;
import api.queries.Reducer52;

public enum CensusQuery {
	QUERY_1, QUERY_2, QUERY_3, QUERY_4, QUERY_5;

	private static final String STATE_MAP_NAME = "54080-54265-states";

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
		Path file = Paths.get(System.getProperty("outPath"));
		try {
			switch (this) {

			case QUERY_1:

				ICompletableFuture<Map<AgeCategory, Integer>> future1 = job.mapper(new Mapper1())
						.reducer(new Reducer1()).submit();

				Set<Entry<AgeCategory, Integer>> ans1 = future1.get().entrySet();
				List<String> list = new LinkedList<String>();
				for (Entry<AgeCategory, Integer> entry : ans1) {
					list.add(entry.getKey() + " = " + entry.getValue());
				}
				System.out.println(ans1);
				break;

			case QUERY_2:

				ICompletableFuture<Map<Integer, Double>> future2 = job.mapper(new Mapper2()).reducer(new Reducer2())
						.submit();
				Map<Integer, Double> ans2 = future2.get();
				System.out.println(ans2.values());
				break;

			case QUERY_3:

				ICompletableFuture<Map<String, Double>> future3 = job.mapper(new Mapper3()).reducer(new Reducer3())
						.submit();
				Map<String, Double> ans3 = future3.get();
				System.out.println(ans3.values());
				break;

			case QUERY_4:

				ICompletableFuture<Map<String, Integer>> future4 = job.mapper(new Mapper4()).reducer(new Reducer4())
						.submit();
				Map<String, Integer> ans4 = future4.get();
				System.out.println(ans4.values());
				break;

			case QUERY_5:

				ICompletableFuture<Map<String, Integer>> auxFuture = job.mapper(new Mapper4()).reducer(new Reducer51())
						.submit();
				Set<Entry<String, Integer>> states = auxFuture.get().entrySet();

				IMap<String, Integer> stateMap = client.getMap(STATE_MAP_NAME);

				for (Entry<String, Integer> entry : states) {
					stateMap.set(entry.getKey(), entry.getValue());
				}

				KeyValueSource<String, Integer> source2 = KeyValueSource.fromMap(stateMap);
				Job<String, Integer> job2 = tracker.newJob(source2);
				ICompletableFuture<Map<Integer, List<StatePair>>> future5 = job2.mapper(new Mapper5())
						.reducer(new Reducer52()).submit();
				Map<Integer, List<StatePair>> ans5 = future5.get();
				System.out.println(ans5.values());
				break;
			}
		} catch (InterruptedException e) {
			e.printStackTrace();
		} catch (ExecutionException e) {
			e.printStackTrace();
		}

	}

	public void writeFile(Path file, List<String> lines) {
		try {
			Files.write(file, lines, Charset.forName("UTF-8"));
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
