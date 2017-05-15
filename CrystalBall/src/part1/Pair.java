package part1;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

public class Pair implements Writable, WritableComparable<Pair> {
	private Text first;
	private Text second;

	public Pair() {
		this.first = new Text();
		this.second = new Text();
	}

	public Pair(Text first, Text second) {
		this.first = first;
		this.second = second;
	}

	public Text getFirst() {
		return first;
	}

	public void setFirst(String first) {
		this.first.set(first);
	}

	public Text getSecond() {
		return second;
	}

	public void setSecond(String second) {
		this.second.set(second);
	}

	public static Pair read(DataInput in) throws IOException {
		Pair pair = new Pair();
		pair.readFields(in);
		return pair;

	}

	@Override
	public void readFields(DataInput in) throws IOException {
		first.readFields(in);
		second.readFields(in);

	}

	@Override
	public void write(DataOutput out) throws IOException {
		first.write(out);
		second.write(out);

	}

	@Override
	public int compareTo(Pair o) {

		if (!this.first.toString().equals(o.first.toString()))
			return this.first.toString().compareTo(o.first.toString());

		return this.second.toString().compareTo(o.second.toString());
	}

	@Override
	public int hashCode() {
		int result = first != null ? first.hashCode() : 0;
		result = 163 * result + (second != null ? second.hashCode() : 0);
		return result;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o)
			return true;
		if (o == null || getClass() != o.getClass())
			return false;

		Pair pair = (Pair) o;

		if (second != null ? !second.equals(pair.second) : pair.second != null)
			return false;
		if (first != null ? !first.equals(pair.first) : pair.first != null)
			return false;

		return true;
	}

	@Override
	public String toString() {
		return "<" + first + ", " + second + ">";
	}

}
