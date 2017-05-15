package part1;

import java.io.IOException;
import java.util.Map.Entry;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Mapper;

public class PairMapper extends Mapper<LongWritable, Text, Pair, IntWritable> {
	private Pair pair = new Pair();
	private IntWritable ONE = new IntWritable(1);
	private MapWritable map = new MapWritable();

	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String[] terms = value.toString().split("\\s+");
		for (int i = 0; i < terms.length; i++) {

			for (int j = i + 1; j < terms.length && !terms[i].equals(terms[j]); j++) {

				pair = new Pair(new Text(terms[i]), new Text(terms[j]));
				IntWritable pairValue = map.get(pair) == null ? ONE
						: new IntWritable(
								(((IntWritable) map.get(pair)).get() + 1));

				map.put(pair, pairValue);

				pair = new Pair(new Text(terms[i]), new Text("*"));
				IntWritable starValue = map.get(pair) == null ? ONE
						: new IntWritable(
								(((IntWritable) map.get(pair)).get() + 1));

				map.put(pair, starValue);

			}
		}

	}

	@Override
	protected void cleanup(Context context) throws IOException,
			InterruptedException {

		for (Entry<Writable, Writable> entry : map.entrySet()) {

			Pair key = (Pair) entry.getKey();
			IntWritable value = (IntWritable) entry.getValue();
			context.write(key, value);
		}
	}
}
