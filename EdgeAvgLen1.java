package comp9313.ass1;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class EdgeAvgLen1 {

	public static class EndpointWeightMapper extends Mapper<Object, Text, Text, DoubleWritable> {

		private final static Text endpoint = new Text();
		private final static DoubleWritable weight = new DoubleWritable();

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			StringTokenizer itr = new StringTokenizer(value.toString(), " *$&#/\t\n\f\"'\\,:;?![](){}<>~-_");
			int cnt = 1;
			while (itr.hasMoreTokens()) {
				String token = itr.nextToken();
				if (cnt == 1 || cnt == 2) {
                    // token refers to index when cnt == 1
					// token refers to startpoint when cnt == 2
				} else if (cnt == 3) {
					endpoint.set(token);
				} else if (cnt == 4) {
					weight.set(new Double(token).doubleValue());
				} else {
                    // format error
				}
				context.write(endpoint, weight);
				cnt += 1;
			}
		}
	}

	public static class EvgWeightReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {

	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "");
		job.setJarByClass(EdgeAvgLen1.class);
		job.setMapperClass(EndpointWeightMapper.class);
		job.setReducerClass(EvgWeightReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);

	}
}
