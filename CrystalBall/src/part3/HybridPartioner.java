package part3;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class HybridPartioner extends Partitioner<Text, IntWritable> {

	@Override
	public int getPartition(Text key, IntWritable value, int numReduceTasks) {
		String temp = key.toString();
		int tempKey = Integer.parseInt(temp.substring(0, temp.indexOf('.')));

		if (numReduceTasks == 0)
			return 0;

		if (tempKey < 40)
			return 0;
		else
			return 1 % numReduceTasks;

	}
}