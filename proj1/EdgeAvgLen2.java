package comp9313.ass1;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map.Entry;
import java.util.Set;
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


public class EdgeAvgLen2 {
	/*
	 * class "EdgeAvgLen2" implements <key, average-weight> calculation algorithm, using in-mapping combiner
	 * 
	 *  Here, reducer is remained from EdgeAvgLen1
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

	public static class EndpointWeightMapper extends Mapper<Object, Text, IntWritable, CntAvgTuple> {
		/*
		 * class "EndpointWeightMapper" extends Mapper.
		 * mapper-in		text line 	 
		 * mapper-out		<endpoint: (cnt, avg)>
		 */
		
		private IntWritable endpoint = new IntWritable();
		private static CntAvgTuple cntAvg = new CntAvgTuple();

		private static HashMap<Integer, CntAvgTuple> map = new HashMap<Integer, CntAvgTuple>();
		
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			StringTokenizer itr = new StringTokenizer(value.toString(), " *$&#/\t\n\f\"'\\,:;?![](){}<>~-_");
			int _endpoint = 0;
			double _weight = 0.0;
			int flag = 1;
			while (itr.hasMoreTokens()) {
				String token = itr.nextToken();
				if (flag == 1 || flag == 2) { //token refers to line-no. or start-point
					// pass
				} else if (flag == 3) { //token refers to end-point
					_endpoint = new Integer(token).intValue();
				} else if (flag == 4) { //token refers to weight of edge
					_weight = new Double(token).doubleValue();
				} else {
                    // format error: more than 4 values inner one line
				}
				flag += 1;
			}
			//System.out.println(endpoint.toString() + " -> " + cntAvg.toString());
			if(map.containsKey(_endpoint)){
				CntAvgTuple _cntAvg = map.get(_endpoint);
				// calculate accumulated average-weight as weight-info (cnt, avg)
				_cntAvg.setAvg((_cntAvg.getAvg()*_cntAvg.getCnt() + _weight)/(_cntAvg.getCnt()+1));
				_cntAvg.setCnt(_cntAvg.getCnt() + 1);
				map.put(_endpoint, _cntAvg);
			}else {
			    CntAvgTuple _cntAvg = new CntAvgTuple();
			    _cntAvg.set(1, _weight);
				map.put(_endpoint, _cntAvg);
			}
			
		}
		
		public void cleanup(Context context) throws IOException, InterruptedException {
			Set<Entry<Integer, CntAvgTuple>> sets = map.entrySet();
			for(Entry<Integer, CntAvgTuple> entry: sets){
				endpoint.set(entry.getKey());
				CntAvgTuple val = entry.getValue();
				cntAvg.set(val.getCnt(), val.getAvg());
				context.write(endpoint, cntAvg);
			}
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
		Job job = Job.getInstance(conf, "jab EdgeAvLen2");
		job.setJarByClass(EdgeAvgLen2.class);
		job.setMapperClass(EndpointWeightMapper.class);
		job.setReducerClass(EvgWeightReducer.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(CntAvgTuple.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);

	}
	
}
