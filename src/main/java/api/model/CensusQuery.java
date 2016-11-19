package api.model;

import java.util.Map;
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
import api.queries.Reducer1;
import api.queries.Reducer2;
import api.queries.Reducer3;
import api.queries.Reducer4;

public enum CensusQuery {
	QUERY_1, QUERY_2, QUERY_3, QUERY_4, QUERY_5;

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
		try {
			switch (this) {
			case QUERY_1:
				ICompletableFuture<Map<AgeCategory, Integer>> future1 = job.mapper(new Mapper1())
						.reducer(new Reducer1()).submit();

				Map<AgeCategory, Integer> ans1 = future1.get();
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
				ICompletableFuture<Map<String, Integer>> future5 = job.mapper(new Mapper4()).reducer(new Reducer4())
						.submit();
				Map<String, Integer> ans5 = future5.get();
				System.out.println(ans5.values());
				break;
			}
		} catch (InterruptedException e) {
			e.printStackTrace();
		} catch (ExecutionException e) {
			e.printStackTrace();
		}

	}
}
