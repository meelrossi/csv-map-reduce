package client;

import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;

import org.supercsv.cellprocessor.ParseInt;
import org.supercsv.cellprocessor.Trim;
import org.supercsv.cellprocessor.constraint.NotNull;
import org.supercsv.cellprocessor.ift.CellProcessor;
import org.supercsv.io.CsvBeanReader;
import org.supercsv.io.ICsvBeanReader;
import org.supercsv.prefs.CsvPreference;

import com.hazelcast.core.IMap;

import api.model.Census;

public class CensusReader {

	private static final String FILENAME = "./files/dataset-1000.csv";

	private static CellProcessor[] getProcessors() {
		return new CellProcessor[] {
				// Tipo de vivienda
				new ParseInt(new NotNull()),
				// Calidad Servicios
				null,
				// Sexo
				null,
				// Edad
				new ParseInt(new NotNull()),
				// Alfabetismo
				new ParseInt(new NotNull()),
				// Actividad
				null,
				// Nombre departamento
				new Trim(new NotNull()),
				// Nombre Provincia
				new Trim(new NotNull()),
				// Hogar id
				new ParseInt(new NotNull()) };
	}

	public static void readCensus(final IMap<String, Census> theIMap) throws Exception {
		ICsvBeanReader beanReader = null;
		String path = System.getProperty("inPath");
		if (path == null) path = FILENAME;
		
		try {
			final InputStream is = CensusReader.class.getClassLoader().getResourceAsStream(path);
			final Reader aReader = new InputStreamReader(is);
			beanReader = new CsvBeanReader(aReader, CsvPreference.STANDARD_PREFERENCE);

			final String[] header = { "houseType", null, null, "age", "literacy", null,
					"countyName", "stateName", "houseId" };
			final CellProcessor[] processors = getProcessors();

			Census aV;
			beanReader.getHeader(true);
			theIMap.clear();
			Integer index = 0;
			while ((aV = beanReader.read(Census.class, header, processors)) != null) {
				if (shouldAdd(aV)) {
					theIMap.set(index.toString(), aV);
					index++;
				}
			}
		} finally {
			if (beanReader != null) {
				beanReader.close();
			}
		}
	}
	
	public static boolean shouldAdd(Census census) {
		if (Integer.parseInt(System.getProperty("query")) == 4) {
			String prov = System.getProperty("prov");
			if (prov == null) throw new IllegalArgumentException();
			return prov.equalsIgnoreCase(census.getStateName());
		}
		return true;
	}
}
