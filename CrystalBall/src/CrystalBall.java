import java.io.IOException;
import java.util.Scanner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import part1.Pair;
import part1.PairMapper;
import part1.PairPartioner;
import part1.PairReducer;
import part2.StripesMapper;
import part2.StripesPartioner;
import part2.StripesReducer;
import part3.HybridMapper;
import part3.HybridPartioner;
import part3.HybridReducer;

public class CrystalBall {

	public static void main(String[] args) throws IOException,
			ClassNotFoundException, InterruptedException {
		System.out.println("Select a metohd");
		System.out.println("1 for Pair Approach");
		System.out.println("2 for Stripes Approach");
		System.out.println("3 for Hybrid Approach");

		Scanner s = new Scanner(System.in);
		int a = Integer.parseInt(s.nextLine());
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "Crystal ball");
		Path input = new Path("CrytalBallInput");
		switch (a) {
		case 1:
			job.setJarByClass(CrystalBall.class);
			
			Path pairOutput = new Path("Pair_output");
			FileInputFormat.addInputPath(job, input);
			FileOutputFormat.setOutputPath(job, pairOutput);

			job.setMapperClass(PairMapper.class);
			job.setReducerClass(PairReducer.class);
			job.setMapOutputKeyClass(Pair.class);
			job.setMapOutputValueClass(IntWritable.class);
			job.setOutputKeyClass(Pair.class);
			job.setOutputValueClass(DoubleWritable.class);
			job.setPartitionerClass(PairPartioner.class);

			job.setNumReduceTasks(2);

			System.exit(job.waitForCompletion(true) ? 0 : 1);
			break;
		case 2:
			
			job.setJarByClass(CrystalBall.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(MapWritable.class);
			
			Path stripesOutput = new Path("Stripes_output");
			FileInputFormat.addInputPath(job, input);
			FileOutputFormat.setOutputPath(job, stripesOutput);

			job.setMapperClass(StripesMapper.class);
			job.setReducerClass(StripesReducer.class);
			job.setPartitionerClass(StripesPartioner.class);
			
			job.setInputFormatClass(TextInputFormat.class);
			job.setOutputFormatClass(TextOutputFormat.class);
			

			job.setNumReduceTasks(2);

			System.exit(job.waitForCompletion(true) ? 0 : 1);
			break;
		case 3:
			job.setJarByClass(CrystalBall.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(IntWritable.class);
			
			Path hybridOutput = new Path("Hybrid_output");
			FileInputFormat.addInputPath(job, input);
			FileOutputFormat.setOutputPath(job, hybridOutput);

			job.setMapperClass(HybridMapper.class);
			job.setReducerClass(HybridReducer.class);
			job.setPartitionerClass(HybridPartioner.class);
			
			job.setInputFormatClass(TextInputFormat.class);
			job.setOutputFormatClass(TextOutputFormat.class);
			

			job.setNumReduceTasks(2);

			System.exit(job.waitForCompletion(true) ? 0 : 1);
			break;

		default:
			System.out.println("Please Select a metohd");
			System.out.println("1 for Pair Approach");
			System.out.println("2 for Stripes Approach");
			System.out.println("3 for Hybrid Approach");
		}
		
		s.close();
	}

	

}
