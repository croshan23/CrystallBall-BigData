package part3;

import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class HybridMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
	class Pairs {
		String pair1;
		String pair2;

		public Pairs(String pair1, String pair2) {
			this.pair1 = pair1;
			this.pair2 = pair2;
		}

		public String toString() {
			return pair1 + "." + pair2;
		}
	}

	java.util.Map<Pairs, Integer> pairData = new HashMap<>();

	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {

		String[] splittedData = value.toString().split("\\s+");

		for (int sdIndex = 0; sdIndex < splittedData.length - 1; sdIndex++) {

			for (int neighborIndex = sdIndex + 1; neighborIndex < splittedData.length; neighborIndex++) {

				String element1 = splittedData[sdIndex];
				String element2 = splittedData[neighborIndex];

				if (element1.equals(element2)) {
					break;
				}

				if (pairData.get(new Pairs(element1, element2)) == null) {
					pairData.put(new Pairs(element1, "*"), 1);
					pairData.put(new Pairs(element1, element2), 1);

				} else {
					pairData.put(new Pairs(element1, "*"),
							pairData.get(new Pairs(element1, "*")) + 1);
					pairData.put(new Pairs(element1, element2),
							pairData.get(new Pairs(element1, element2)) + 1);
				}
			}
		}
	}

	@Override
	protected void cleanup(Context context) throws IOException,
			InterruptedException {
		for (java.util.Map.Entry<Pairs, Integer> entry : pairData.entrySet()) {
			System.out.println(entry.getKey() + " " + entry.getValue());
			context.write(new Text(entry.getKey().toString()), new IntWritable(
					entry.getValue()));
		}
	}
}