package part3;

import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class HybridReducer extends Reducer<Text, IntWritable, Text, Text> {

	java.util.Map<String, Double> pairData = new HashMap<>();
	String currentVal = null;
	double marginal = 0;

	@Override
	public void reduce(Text key, Iterable<IntWritable> values, Context context)
			throws IOException, InterruptedException {
		String keyData = key.toString();
		String newElem = keyData.substring(0, keyData.indexOf('.'));// 1

		if (currentVal == null)
			currentVal = newElem;

		if (!currentVal.equals(newElem)) {
			// sending task here
			for (java.util.Map.Entry<String, Double> entry : pairData
					.entrySet()) {
				entry.setValue(Math.round((entry.getValue() / marginal * 100) * 100.0) / 100.0);
			}
			context.write(new Text(currentVal), new Text(pairData.toString()));
			marginal = 0;
			pairData.clear();// 2-
			currentVal = newElem;
		}

		for (IntWritable val : values) {
			if (keyData.charAt(key.toString().indexOf('.') + 1) == '*') {
				marginal += val.get();
			} else {
				String nextElement = keyData
						.substring(keyData.indexOf('.') + 1);
				if (pairData.get(nextElement) == null) {
					pairData.put(nextElement, (double) val.get());
				} else {
					pairData.put(nextElement,
							pairData.get(nextElement) + val.get());
				}
			}
		}
	}

	protected void cleanup(Context context) throws IOException,
			InterruptedException {
		for (java.util.Map.Entry<String, Double> entry : pairData.entrySet()) {
			entry.setValue(Math.round((entry.getValue() / marginal * 100) * 100.0) / 100.0);
		}
		context.write(new Text(currentVal), new Text(pairData.entrySet()
				.toString()));
	}
}