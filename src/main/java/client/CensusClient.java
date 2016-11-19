package client;

import java.util.Map;
import java.util.concurrent.ExecutionException;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.config.ClientNetworkConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;

import api.model.Census;
import api.model.CensusQuery;
import api.utils.ArgsParser;

public class CensusClient {
	private static final String MAP_NAME = "54080-54265-census";

	public static void main(String[] args) throws InterruptedException, ExecutionException {

		String name = System.getProperty("name");
		if (name == null) {
			name = "dev";
		}

		String pass = System.getProperty("pass");
		if (pass == null) {
			pass = "dev-pass";
		}
		System.out.println(String.format("Connecting with cluster %s", name));

		ClientConfig ccfg = new ClientConfig();
		ccfg.getGroupConfig().setName(name).setPassword(pass);

		String addresses = System.getProperty("addresses");
		if (addresses != null) {
			String[] arrayAddresses = addresses.split("[,;]");
			ClientNetworkConfig net = new ClientNetworkConfig();
			net.addAddress(arrayAddresses);
			ccfg.setNetworkConfig(net);
		}
		HazelcastInstance client = HazelcastClient.newHazelcastClient(ccfg);

		IMap<String, Census> myMap = client.getMap(MAP_NAME);

		try {
			CensusReader.readCensus(myMap);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}

		Map<String, Object> properties = ArgsParser.parseArgs(args);
		CensusQuery query = CensusQuery.getQuery(Integer.parseInt((String) properties.get("QUERY")));
		
		query.run(client, myMap);

        System.exit(0);

	}
}
