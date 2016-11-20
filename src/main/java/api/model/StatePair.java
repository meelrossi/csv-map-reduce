package api.model;

import java.io.IOException;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;

public class StatePair implements DataSerializable {

	private CountyState countyState1;
	private CountyState countyState2;

	public StatePair(CountyState countyState1, CountyState countyState2) {
		this.countyState1 = countyState1;
		this.countyState2 = countyState2;
	}
	
	public StatePair() {
		super();
	}

	@Override
	public void writeData(ObjectDataOutput out) throws IOException {
		out.writeObject(countyState1);
		out.writeObject(countyState2);
	}

	@Override
	public void readData(ObjectDataInput in) throws IOException {
		countyState1 = in.readObject();
		countyState2 = in.readObject();
	}

	public CountyState getCountyState1() {
		return countyState1;
	}

	public void setCountyState1(CountyState countyState1) {
		this.countyState1 = countyState1;
	}

	public CountyState getCountyState2() {
		return countyState2;
	}

	public void setCountyState2(CountyState countyState2) {
		this.countyState2 = countyState2;
	}

}
