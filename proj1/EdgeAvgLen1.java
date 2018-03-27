package comp9313.ass1;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class EdgeAvgLen1 {
	/*
	 * class "EdgeAvgLen1" implements <key, average-weight> calculation algorithm, using combiner
	 * 
	 * There are two method to implement the <key, average-weight> algorithm, 
	 *     the first method use (cnt, avg) tuple as weight info,
	 *     the second method use (cnt, sum) tuple as weight info.
	 * In the first method, implement of combiner and reducer is quit same except the output format,
	 * but quit different in the second method. The second method tends to be a bit little quicker 
	 * than the first one due to avoidance of intermediate average calculation
	 * 
	 * 
	 * First method (cnt, avg)
	 * 
	 * mapper-in		text line 	 
	 * mapper-out		<endpoint: (cnt, avg)>
	 *
	 * combiner-in		<endpoint: [(cnt, avg), ... ]>
	 * combiner-out		<endpoint: [(cnt, avg), ... ]>
	 *
	 * reducer-in		<endpoint: [(cnt, avg), ... ]>
	 * reducer-out		<endpoint: avg>
	 *
	 *
	 * Second method (cnt, sum)
	 * 
	 * mapper-in		text line 	 
	 * mapper-out		<endpoint: (cnt, sum)>
	 *
	 * combiner-in		<endpoint: [(cnt, sum), ... ]>
	 * combiner-out		<endpoint: [(cnt, sum), ... ]>
	 *
	 * reducer-in		<endpoint: [(cnt, sum), ... ]>
	 * reducer-out		<endpoint: avg>
	 * 
	 * 
	 * Here, the first method is implemented.
	 */ 
	
	public static class CntAvgTuple implements Writable {
		/*
		 * class "CntAvgTuple" constructs a tuple structure of <cnt, avg> to keep weight info.
		 *     cnt - count
		 *     avg - average weight
		 */
		
		private int cnt;
		private double avg;
				
		public int getCnt() {
			return cnt;
		}

		public void setCnt(int cnt) {
			this.cnt = cnt;
		}

		public double getAvg() {
			return avg;
		}

		public void setAvg(double avg) {
			this.avg = avg;
		}
		
		public CntAvgTuple(){
			this.cnt = 0;
			this.avg = 0.0;
		}
		
		public String toString() {
			return "(" + this.cnt + "," + this.avg + ")";
		}

		@Override
		public void readFields(DataInput in) throws IOException {
			cnt = in.readInt();
			avg = in.readDouble();
		}

		@Override
		public void write(DataOutput out) throws IOException {
			out.writeInt(cnt);
			out.writeDouble(avg);			
		}

		public void set(int cnt, double avg) {
			setCnt(cnt);
			setAvg(avg);
		}
		
	}

	public static class CntSumTuple implements Writable {
		private int cnt;
		private double sum;
		
		public int getCnt() {
			return cnt;
		}

		public void setCnt(int cnt) {
			this.cnt = cnt;
		}

		public double getSum() {
			return sum;
		}

		public void setSum(double sum) {
			this.sum = sum;
		}
		
		public String toString() {
			return "(" + this.cnt + "," + this.sum + ")";
		}

		@Override
		public void readFields(DataInput in) throws IOException {
			cnt = in.readInt();
			sum = in.readDouble();
		}

		@Override
		public void write(DataOutput out) throws IOException {
			out.writeInt(cnt);
			out.writeDouble(sum);
		}
		
	}
	
	
	public static class EndpointWeightMapper extends Mapper<Object, Text, IntWritable, CntAvgTuple> {
		/*
		 * class "EndpointWeightMapper" extends Mapper.
		 * mapper-in		text line 	 
		 * mapper-out		<endpoint: (cnt, avg)>
		 */
		
		private IntWritable endpoint = new IntWritable();
		private static CntAvgTuple cntAvg = new CntAvgTuple();

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			StringTokenizer itr = new StringTokenizer(value.toString(), " *$&#/\t\n\f\"'\\,:;?![](){}<>~-_");
			int flag = 1;
			while (itr.hasMoreTokens()) {
				String token = itr.nextToken();
				if (flag == 1 || flag == 2) {  //token refers to line-no. or start-point
					// pass
				} else if (flag == 3) {  //token refers to end-point
					endpoint.set(new Integer(token).intValue());
				} else if (flag == 4) {  //token refers to weight of edge
					cntAvg.set(1, new Double(token).doubleValue());
				} else {
                    // format error: more than 4 values inner one line
				}
				flag += 1;
			}
			//System.out.println(endpoint.toString() + " -> " + cntAvg.toString());
			context.write(endpoint, cntAvg);
			
		}
	}

	
	public static class EvgWeightCombiner extends Reducer<IntWritable, CntAvgTuple, IntWritable, CntAvgTuple> {
		/*
		 * class "EvgWeightCombiner" extends Reducer.
		 * combiner-in		<endpoint: [(cnt, avg), ... ]>
		 * combiner-out		<endpoint: [(cnt, avg), ... ]>
		 */
		
		private CntAvgTuple cntAvg = new CntAvgTuple();
		
		public void reduce(IntWritable key, Iterable<CntAvgTuple> values, Context context) throws IOException, InterruptedException {
			int cnt = 0;
			double sum = 0;
			for (CntAvgTuple val : values) {
				cnt += val.getCnt();
				sum += val.getCnt()*val.getAvg();
			}
			cntAvg.set(cnt, sum/cnt);
			//System.out.println("Combiner:" + key.toString() + " --> " + cntAvg.toString());
			context.write(key, cntAvg);
		}
	}
	
	public static class EvgWeightReducer extends Reducer<IntWritable, CntAvgTuple, IntWritable, DoubleWritable> {
		/*
		 * class "EndpointWeightReducer" extends Reducer.
		 * reducer-in		<endpoint: [(cnt, avg), ... ]>
		 * reducer-out		<endpoint: avg>
		 */
		
		private CntAvgTuple cntAvg = new CntAvgTuple();
		
		public void reduce(IntWritable key, Iterable<CntAvgTuple> values, Context context) throws IOException, InterruptedException {
			int cnt = 0;
			double sum = 0;
			for (CntAvgTuple val : values) {
				cnt += val.getCnt();
				sum += val.getCnt()*val.getAvg();
			}
			cntAvg.set(cnt, sum/cnt);
			//System.out.println("Combiner:" + key.toString() + " --> " + cntAvg.toString());
			context.write(key, new DoubleWritable(cntAvg.getAvg()));
		}
	}
	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "job EdgeAvLen1");
		job.setJarByClass(EdgeAvgLen1.class);
		job.setMapperClass(EndpointWeightMapper.class);
		job.setCombinerClass(EvgWeightCombiner.class);
		job.setReducerClass(EvgWeightReducer.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(CntAvgTuple.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);

	}
}
