import java.io.*;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MVMul {

	// The Mapper: map lines of the matrix to target index and Mij*Vj values 
	public static class MVMulMapper
	extends Mapper<Object, Text, IntWritable, FloatWritable>{
		private IntWritable outputIndex = new IntWritable();
		private FloatWritable mij = new FloatWritable();

		private ArrayList<Float> vectorElements = null;

		private void readVector() throws IOException
		{
			if (vectorElements != null) {
				// Already read
				return;
			}

			vectorElements = new ArrayList<Float>();

			// Read the vector file
			Path pt = new Path("hdfs:/shared/vector.txt");
			FileSystem fs = FileSystem.get(new Configuration());
			BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(pt)));
			String line = br.readLine();

			while (line != null) {
				vectorElements.add(Float.valueOf(line));
				line = br.readLine();
			}
		}

		public void map(Object key, Text value, Context context
		) throws IOException, InterruptedException
		{
			readVector();

			String data = value.toString();
			String[] lines = data.split("\n");

			for (String line : lines) {
				System.out.println(line);

				String[] parts = line.split(" ");
				int row = Integer.parseInt(parts[0]);
				int col = 0;

				// Read the columns
				for (int i=1; i<parts.length; i++) {
					float v = Float.parseFloat(parts[i]);

					// Emit Mij*Vj
					outputIndex.set(row);
					mij.set(v * vectorElements.get(col));
					context.write(outputIndex, mij);

					col += 1;
				}
			}
		}
	}

public static class MVMulReducer
extends Reducer<IntWritable,FloatWritable,IntWritable,FloatWritable> {
	private FloatWritable result = new FloatWritable();
	
	public void reduce(IntWritable key, Iterable<FloatWritable> values, Context context
	) throws IOException, InterruptedException {
		// key is an output index
		// values contains all the Mij*Vj values used to produce the output
		float total = 0.0f;

		for (FloatWritable val : values) {
			total += val.get();
		}

		result.set(total);
		context.write(key, result);
	}
}

public static void main(String[] args) throws Exception {
	Configuration conf = new Configuration();
	Job job = Job.getInstance(conf, "float average");
	
	// Pipeline of stages
	job.setJarByClass(MVMul.class);      // NOTE HERE
	job.setMapperClass(MVMulMapper.class);
	job.setCombinerClass(MVMulReducer.class);
	job.setReducerClass(MVMulReducer.class);
	
	// Description of what the reducer produces (what keys it uses, what values it uses)
	job.setOutputKeyClass(IntWritable.class);
	job.setOutputValueClass(FloatWritable.class);
	
	FileInputFormat.addInputPath(job, new Path(args[0]));
	FileOutputFormat.setOutputPath(job, new Path(args[1]));
	System.exit(job.waitForCompletion(true) ? 0 : 1);
}

}
