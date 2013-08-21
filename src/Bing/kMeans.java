package Bing;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
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
	public static final int NITERATION = 20;
	public static final double THRESH = 0.01;
	public static final int NCLUSTER = 3;
	public static final int DIMENSION = 2;
	public static double[][] center;
	public static double[][] precenter;
	public static class Map extends MapReduceBase implements
			Mapper<LongWritable, Text, IntWritable,DoubleArrayWritable> {

		public void map(LongWritable key, Text value,
				OutputCollector<IntWritable, DoubleArrayWritable> output, Reporter reporter)
				throws IOException {
			String line = value.toString();
			String[] numbers = line.split(" ");
			double [] point = new double[DIMENSION];
			DoubleWritable [] wPoint = new DoubleWritable[DIMENSION];
			for (int j = 0; j < DIMENSION; j++){
				point[j] = Double.parseDouble(numbers[j]);
				wPoint[j] = new DoubleWritable(point[j]);
			}

			double distance;
			int cluster=0;
			double min = Math.sqrt(DIMENSION);
			for (int i = 0; i<NCLUSTER; i++) {
				double sum = 0;
				for (int j = 0; j < DIMENSION; j++){
					sum += (point[j] - center[i][j]) * (point[j] - center[i][j]);
					System.out.println("map point[j]= "+point[j] + " center[i][j]= "+center[i][j]);
				}
				distance = Math.sqrt(sum); 
				if (min>distance){
					cluster = i;
					min = distance;
				}
			}
			DoubleArrayWritable dAWP = new DoubleArrayWritable();
			dAWP.set(wPoint);
			output.collect(new IntWritable(cluster), dAWP);
		}
	}

	public static class Reduce extends MapReduceBase implements
			Reducer<IntWritable, DoubleArrayWritable, IntWritable, DoubleArrayWritable> {
		public void reduce(IntWritable key, Iterator<DoubleArrayWritable> values,
				OutputCollector<IntWritable, DoubleArrayWritable> output, Reporter reporter)
				throws IOException {
			double[] sum = new double[DIMENSION];
			int nPoints = 0;
			while (values.hasNext()) {
				Writable[] point = values.next().get();
				for (int j = 0; j < DIMENSION; j++){
					sum[j] += ((DoubleWritable) point[j]).get();
				}
				nPoints++;
				
			}
			System.out.println("reduce key= "+key + " nPoints= "+nPoints);
			DoubleWritable [] dwCenter = new DoubleWritable[DIMENSION];
			for (int j = 0; j < DIMENSION; j++){
				center[key.get()][j] = sum[j]/nPoints;
				dwCenter[j] = new DoubleWritable(center[key.get()][j]);
			}
			
			
			DoubleArrayWritable dAWP = new DoubleArrayWritable();
			dAWP.set(dwCenter);
			output.collect(key, dAWP);
		}
	}

	public static void main(String[] args) throws Exception {
		 Process process = Runtime.getRuntime().exec("rm -rf /home/cloudera/workspace/training/dataoutput");
		center = new double[NCLUSTER][DIMENSION];
		precenter = new double[NCLUSTER][DIMENSION];
		String inputfile = "file:///home/cloudera/workspace/training/datasource/kmean";
		String outputfile = "file:///home/cloudera/workspace/training/dataoutput/iteration0";
		for (int i = 0; i<NCLUSTER;i++){
			for (int j = 0; j < DIMENSION; j++){
				center[i][j] = ((double)(i + 1)) / (NCLUSTER + 1);
				precenter[i][j] = center[i][j];
			}
		}
		
		String outFilenamec = "/home/cloudera/workspace/training/datasource/centerInit";
		File filec = new File(outFilenamec);
			
			if (!filec.exists()) {
				try {
					filec.createNewFile();
					FileWriter fwc;
				fwc = new FileWriter(filec.getAbsoluteFile());
				BufferedWriter bwc = new BufferedWriter(fwc);
				for (int i = 0; i< NCLUSTER; i++) {
				bwc.write(center[i][0] + " " + center[i][1]);bwc.newLine();
				}
				bwc.close();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		runJob(new Path(inputfile), new Path(outputfile));
	}

	public static void runJob(Path input, Path output) throws IOException {
		JobConf conf = new JobConf(kMeans.class);
		conf.setJobName("KMEANS");
		conf.setOutputKeyClass(IntWritable.class);
		conf.setOutputValueClass(DoubleArrayWritable.class);
		conf.setMapperClass(Map.class);
		conf.setCombinerClass(Reduce.class);
		conf.setReducerClass(Reduce.class);
		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);
		FileInputFormat.setInputPaths(conf, input);
		System.out.println("star running job");
		StringBuilder strb = new StringBuilder();
		strb.append(output.toString());
		for(int k =1;k<NITERATION;k++){
		strb.replace(strb.length()-1,strb.length(),Integer.toString(k));
		System.out.println(strb);
		FileOutputFormat.setOutputPath(conf, new Path(strb.toString()));
		JobClient.runJob(conf);
		double disSum = 0;
		for (int i = 0; i<NCLUSTER;i++){
			for (int j = 0; j < DIMENSION; j++){
				disSum += (precenter[i][j] - center[i][j]) * (precenter[i][j] - center[i][j]);
			}
		}
		double change = Math.sqrt(disSum);
		System.out.println("change = " + change);
		if (change <THRESH){
			System.out.println("Done iter = " + k);
			break;
		}else{
			for (int i = 0; i<NCLUSTER;i++){
				for (int j = 0; j < DIMENSION; j++){
					precenter[i][j] = center[i][j];
				}
			}
		}//if
	}//for iteration k
	}//run job
}//hello class