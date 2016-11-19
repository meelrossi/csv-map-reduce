package api.utils;

import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;

public class ArgsParser {
	
	public static Map<String, Object> parseArgs(String[] args) {
		System.out.println();
		Map<String, Object> map = new HashMap<String, Object>();
		for (int i = 0; i < args.length; i++) {
			String parameter = args[i];
			@SuppressWarnings("resource")
			Scanner scanner = new Scanner(parameter);
			scanner.useDelimiter("=");
			String vble = scanner.next();
			if (!scanner.hasNext()) {
				System.err.println(String.format("Parameter %s is incorrect", vble));
				continue;
			}
			String value = scanner.next();

			if (vble.trim().equalsIgnoreCase("PORT")) {
				try {
					int port = Integer.parseInt(value);
					map.put("PORT", port);
				} catch (NumberFormatException e) {
					System.err.println(
							String.format("Ignoring the incorrect parameter. %s is not a valid port number", value));
				}
			} else {
				map.put(vble.toUpperCase(), value);
			}
		}
		return map;
	}
}
