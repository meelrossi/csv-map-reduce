package csv.map.reduce.queries;

public enum AgeCategory {
	FROM0TO14, FROM15TO64, FROM65;

	public static AgeCategory getCategory(Integer age) {
		if (age < 15)
			return FROM0TO14;
		if (age < 65)
			return FROM15TO64;
		return FROM65;
	}

	public String getDescription() {
		switch (this) {
		case FROM0TO14:
			return "0-14";
		case FROM15TO64:
			return "15-64";
		case FROM65:
			return "65-?";
		default:
			return "";
		}
	}
}
