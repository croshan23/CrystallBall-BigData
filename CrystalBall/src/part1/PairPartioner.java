package part1;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class PairPartioner extends Partitioner<Pair, IntWritable> {

	@Override
	public int getPartition(Pair key, IntWritable value, int numPartitumions) {
		System.out.println(key.getFirst().toString());
		
		if (numPartitumions == 0)
			return 0;
		if(Integer.parseInt(key.getFirst().toString()) < 40)
			return 0;
		else
			return 1;
	}

}
