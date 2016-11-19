package client;

import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.Map;

import org.supercsv.cellprocessor.ParseInt;
import org.supercsv.cellprocessor.constraint.NotNull;
import org.supercsv.cellprocessor.ift.CellProcessor;
import org.supercsv.io.CsvBeanReader;
import org.supercsv.io.ICsvBeanReader;
import org.supercsv.prefs.CsvPreference;

import api.model.Census;

public class CensusReader {

	private static final String FILENAME = "./files/dataset-1000.csv";

	private static CellProcessor[] getProcessors() {
		return new CellProcessor[] {
				// Tipo de vivienda
				new ParseInt(new NotNull()),
				// Calidad Servicios
				new ParseInt(new NotNull()),
				// Sexo
				new ParseInt(new NotNull()),
				// Edad
				new ParseInt(new NotNull()),
				// Alfabetismo
				new ParseInt(new NotNull()),
				// Actividad
				new ParseInt(new NotNull()),
				// Nombre departamento
				new NotNull(),
				// Nombre Provincia
				new NotNull(),
				// Hogar id
				new ParseInt(new NotNull()) };
	}

	public static void readCensus(final Map<String, Census> theIMap) throws Exception {

		ICsvBeanReader beanReader = null;
		try {
			final InputStream is = CensusReader.class.getClassLoader().getResourceAsStream(FILENAME);
			final Reader aReader = new InputStreamReader(is);
			beanReader = new CsvBeanReader(aReader, CsvPreference.STANDARD_PREFERENCE);

			final String[] header = {"houseType", "serviceQuality", "gender", "age", "literacy", "activity", "countyName", "stateName", "houseId"};
			final CellProcessor[] processors = getProcessors();

			Census aV;
			beanReader.getHeader(true);
			while ((aV = beanReader.read(Census.class, header, processors)) != null) {
				theIMap.put(aV.getHouseId().toString() + aV.getAge(), aV);
			}
		} finally {
			if (beanReader != null) {
				beanReader.close();
			}
		}
	}
}
