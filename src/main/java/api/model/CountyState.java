package api.model;

import java.io.IOException;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;

public class CountyState  implements DataSerializable {
	private String countyName;
	private String stateName;
	
	public CountyState(String countyName, String stateName) {
		this.countyName = countyName;
		this.stateName = stateName;
	}
	
	public CountyState() {
		super();
	}

	@Override
	public void writeData(ObjectDataOutput out) throws IOException {
		out.writeUTF(countyName);
		out.writeUTF(stateName);
	}

	@Override
	public void readData(ObjectDataInput in) throws IOException {
		countyName = in.readUTF();
		stateName = in.readUTF();
	}

	@Override
	public int hashCode() {
		return this.countyName.hashCode();
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		CountyState other = (CountyState) obj;
		if (countyName == null) {
			if (other.countyName != null)
				return false;
		} else if (!countyName.equals(other.getCountyName()))
			return false;
		return true;
	}

	public String getCountyName() {
		return countyName;
	}

	public void setCountyName(String countyName) {
		this.countyName = countyName;
	}

	public String getStateName() {
		return stateName;
	}

	public void setStateName(String stateName) {
		this.stateName = stateName;
	}
	
	
	

}
