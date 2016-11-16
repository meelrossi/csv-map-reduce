package client;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import api.Census;

public class CensusClient {
	public static void main(String[] args) throws InterruptedException, ExecutionException {

		Map<String, Census> myMap = new HashMap<String, Census>();
		try {
			CensusReader.readCensus(myMap);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}
}
