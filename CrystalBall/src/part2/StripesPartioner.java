package part2;

import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class StripesPartioner extends Partitioner<Text, MapWritable> {

	@Override
	public int getPartition(Text key, MapWritable value, int numReduceTasks) {

		int data = Integer.parseInt(key.toString());

		if (numReduceTasks == 0)
			return 0;

		if (data < 40)
			return 0;
		else
			return 1 % numReduceTasks;

	}
}