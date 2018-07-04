package demo;

/**
 * description
 *
 * @author Llx
 * @version v1.0.0
 * @since 2018/7/4
 */
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

//    id 性别   身高
//数据：21 M   139

public class PeopleInfo implements Writable {

    private int peopleID;
    private String gender;
    private int height;

    @Override
    public void readFields(DataInput input) throws IOException {
        this.peopleID = input.readInt();
        this.gender = input.readUTF();
        this.height = input.readInt();
    }

    @Override
    public void write(DataOutput output) throws IOException {
        output.writeInt(peopleID);
        output.writeUTF(gender);
        output.writeInt(height);
    }

    public int getPeopleID() {
        return peopleID;
    }

    public void setPeopleID(int peopleID) {
        this.peopleID = peopleID;
    }

    public String getGender() {
        return gender;
    }

    public void setGender(String gender) {
        this.gender = gender;
    }

    public int getHeight() {
        return height;
    }

    public void setHeight(int height) {
        this.height = height;
    }
}
