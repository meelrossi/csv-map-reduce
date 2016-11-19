package api.queries;

public enum AgeCategory {
	FROM0TO14,
	FROM15TO64,
	FROM65;
	
	public static AgeCategory getCategory(Integer age) {
		if (age < 15) return FROM0TO14;
		if (age < 65) return FROM15TO64;
		return FROM65;
	}
}
