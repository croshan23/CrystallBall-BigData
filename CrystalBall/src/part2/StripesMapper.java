package part2;

import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class StripesMapper extends
		Mapper<LongWritable, Text, Text, MapWritable> {

	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {

		String[] splittedData = value.toString().split("\\s+");

		for (int sdIndex = 0; sdIndex < splittedData.length - 1; sdIndex++) {

			java.util.Map<Text, IntWritable> stripes = new HashMap<>();
			MapWritable toStripes = new MapWritable();

			System.out.print(splittedData[sdIndex] + " >> ");

			for (int neighborIndex = sdIndex + 1; neighborIndex < splittedData.length; neighborIndex++) {
				if (splittedData[neighborIndex].equals(splittedData[sdIndex])) {
					break;
				}
				Text tempIntVal = new Text(splittedData[neighborIndex]);

				if (stripes.get(tempIntVal) == null) {
					stripes.put(tempIntVal, new IntWritable(1));
				} else { // HERE IS THE ERROR
					stripes.put(tempIntVal,
							new IntWritable(stripes.get(tempIntVal).get() + 1));
				}
				System.out.print(splittedData[neighborIndex] + " ");
			}
			System.out.println("");
			System.out
					.println("ROSHN-ROSHAN- " + stripes.entrySet().toString());
			toStripes.putAll(stripes);
			context.write(new Text(splittedData[sdIndex]), toStripes);
		}
		System.out.println("*******************************");
	}

	@Override
	protected void cleanup(Context context) throws IOException,
			InterruptedException {

	}
}