package part1;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class PairReducer extends
		Reducer<Pair, IntWritable, Pair, DoubleWritable> {
	private int totalFreq = 0;

	public void reduce(Pair key, Iterable<IntWritable> values, Context context)
			throws IOException, InterruptedException {

		int sum = sum(values);

		if (key.getSecond().toString().equals("*")) {
			totalFreq = sum;
		} else {
			
			context.write(key, new DoubleWritable(Math.round((((double)sum / totalFreq)) * 100.0) / 100.0));
		}
	}

	public int sum(Iterable<IntWritable> values) {
		int sum = 0;
		for (IntWritable value : values) {
			sum += value.get();
		}
		return sum;
	}
}
