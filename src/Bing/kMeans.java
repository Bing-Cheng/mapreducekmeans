package Bing;

import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;

public class kMeans {
	public static int gInt=0;
	public static double[] center;
	public static double[] precenter;
	public static class Map extends MapReduceBase implements
			Mapper<LongWritable, Text, IntWritable,DoubleWritable> {
		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();

		public void map(LongWritable key, Text value,
				OutputCollector<IntWritable, DoubleWritable> output, Reporter reporter)
				throws IOException {
			String line = value.toString();
			System.out.println(key);
			gInt++;
			System.out.println("map "+gInt);
			double p1 = Double.parseDouble(line);
			System.out.println("p1= "+p1);
			double dis;
			int cl=0;
			double min = 11;
			for (int i = 0; i<3; i++) {
				dis = Math.abs(p1 - center[i]); 
				if (min>dis){
					cl = i;
					min = dis;
				}
				System.out.println("i= "+i+" dis= "+dis);
			}
			
			output.collect(new IntWritable(cl), new DoubleWritable(p1));
		}
	}

	public static class Reduce extends MapReduceBase implements
			Reducer<IntWritable, DoubleWritable, IntWritable, DoubleWritable> {
		public void reduce(IntWritable key, Iterator<DoubleWritable> values,
				OutputCollector<IntWritable, DoubleWritable> output, Reporter reporter)
				throws IOException {
			gInt++;
			System.out.println("reduce "+gInt);
			System.out.println("reduce key "+key);
			double sum = 0;
			int num = 0;
			while (values.hasNext()) {
				sum += values.next().get();
				num++;
				System.out.println("reduce sum= "+sum + " num= "+num);
			}
			double cc = sum/num;
			center[key.get()] = cc;
			output.collect(key, new DoubleWritable(cc));
		}
	}

	public static void main(String[] args) throws Exception {
		// Path input = new Path(args[0]);
		// Path output = new Path(args[1]);
		// runJob(input, output);
		gInt = 5;
		center = new double[3];
		precenter = new double[3];
		for (int i = 0; i<3;i++){
		center[i] = 10*i/3+1;
		precenter[i] = 10*i/3+1;
		}
		System.out.println("main "+gInt);
		runJob(new Path("file:///home/cloudera/sampleData/kmean"), new Path(
				"file:///home/cloudera/output11"));
		System.out.println("main1 "+gInt);
	}

	public static void runJob(Path input, Path output) throws IOException {
		JobConf conf = new JobConf(kMeans.class);
		conf.setJobName("wordcount");
		conf.setOutputKeyClass(IntWritable.class);
		conf.setOutputValueClass(DoubleWritable.class);
		conf.setMapperClass(Map.class);
		conf.setCombinerClass(Reduce.class);
		conf.setReducerClass(Reduce.class);
		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);
		FileInputFormat.setInputPaths(conf, input);
	//	FileOutputFormat.setOutputPath(conf, output);
		System.out.println("runjob "+gInt);
	//	JobClient.runJob(conf);
		System.out.println("runjob1 "+gInt);
		StringBuilder strb = new StringBuilder();
		strb.append("file:///home/cloudera/outputg0");
		for(int j =1;j<10;j++){
		strb.replace(strb.length()-1,strb.length(),Integer.toString(j));
		System.out.println(strb);
		FileOutputFormat.setOutputPath(conf, new Path(
				strb.toString()));
		JobClient.runJob(conf);
		double disSum = 0;
		for (int i = 0; i<3;i++){
			System.out.println("center = "+ center[i]);
			disSum += Math.abs(precenter[i] - center[i]);
		}
		if (disSum <0.2){
			break;
		}else{
			for (int i = 0; i<3;i++){
				precenter[i] = center[i];
			}
		}//if
	}//for j
	}//run job
}//hello class