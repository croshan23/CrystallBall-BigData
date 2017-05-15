package part2;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map.Entry;

import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Reducer;

public class StripesReducer extends Reducer<Text, MapWritable, Text, Text> {

	@Override
	public void reduce(Text key, Iterable<MapWritable> values, Context context)
			throws IOException, InterruptedException {

		double marginal = 0;
		java.util.Map<String, Double> myData = new HashMap<>();

		for (MapWritable value : values) {
			// System.out.println("ROSHANROSHANROSHAN: "+value.entrySet().toString());
			for (Entry<Writable, Writable> val : value.entrySet()) {
				String tempString = val.getKey().toString();
				double tempInt = Integer.parseInt(val.getValue().toString());

				System.out.println("KEY: " + key);
				System.out.println(tempString + " " + tempInt);

				if (myData.get(tempString) == null) {
					myData.put(tempString, tempInt);
				} else {
					myData.put(tempString, myData.get(tempString) + tempInt);
				}

				marginal += tempInt;
			}
			System.out.println(" ");
		}
		System.out.println("Marginal: " + marginal);

		for (java.util.Map.Entry<String, Double> entry : myData.entrySet()) {
			entry.setValue(Math.round((entry.getValue() / marginal * 100) * 100.0) / 100.0);
		}

		context.write(key, new Text(myData.entrySet().toString()));
	}

	@Override
	protected void cleanup(Context context) throws IOException,
			InterruptedException {

	}
}