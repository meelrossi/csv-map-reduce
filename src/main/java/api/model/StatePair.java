package api.model;

import java.io.IOException;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;

public class StatePair implements DataSerializable {
	
	private String state1;
	private String state2;
	
	public StatePair(String state1, String state2) {
		this.state1 = state1;
		this.state2 = state2;
	}
	
	@Override
	public void writeData(ObjectDataOutput out) throws IOException {
		out.writeUTF(state1);
		out.writeUTF(state2);
	}

	@Override
	public void readData(ObjectDataInput in) throws IOException {
		state1 = in.readUTF();
		state2 = in.readUTF();
	}

}
