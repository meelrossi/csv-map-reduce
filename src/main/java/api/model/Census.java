package api.model;

import java.io.IOException;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;

public class Census implements DataSerializable {

	private Integer houseType;
	private Integer serviceQuality;
	private Integer gender;
	private Integer age;
	private Integer literacy;
	private Integer activity;
	private String countyName;
	private String stateName;
	private Integer houseId;

	public void readData(final ObjectDataInput census) throws IOException {
		houseType = census.readInt();
		serviceQuality = census.readInt();
		gender = census.readInt();
		age = census.readInt();
		literacy = census.readInt();
		activity = census.readInt();
		countyName = census.readUTF();
		stateName = census.readUTF();
		houseId = census.readInt();
	}

	public void writeData(ObjectDataOutput census) throws IOException {
		census.writeInt(houseType);
		census.writeInt(serviceQuality);
		census.writeInt(gender);
		census.writeInt(age);
		census.writeInt(literacy);
		census.writeInt(activity);
		census.writeUTF(countyName);
		census.writeUTF(stateName);
		census.writeInt(houseId);
	}

	public Integer getHouseType() {
		return houseType;
	}

	public void setHouseType(Integer houseType) {
		this.houseType = houseType;
	}

	public Integer getServiceQuality() {
		return serviceQuality;
	}

	public void setServiceQuality(Integer serviceQuality) {
		this.serviceQuality = serviceQuality;
	}

	public Integer getGender() {
		return gender;
	}

	public void setGender(Integer gender) {
		this.gender = gender;
	}

	public Integer getAge() {
		return age;
	}

	public void setAge(Integer age) {
		this.age = age;
	}

	public Integer getLiteracy() {
		return literacy;
	}

	public void setLiteracy(Integer literacy) {
		this.literacy = literacy;
	}

	public Integer getActivity() {
		return activity;
	}

	public void setActivity(Integer activity) {
		this.activity = activity;
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

	public Integer getHouseId() {
		return houseId;
	}

	public void setHouseId(Integer houseId) {
		this.houseId = houseId;
	}

}
