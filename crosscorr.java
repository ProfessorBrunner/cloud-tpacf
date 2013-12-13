package org.myorg;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.fs.Path;
//import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat

// ============= my import
import org.apache.hadoop.io.Text;
import java.lang.*;
import java.io.*;
import java.lang.Object;
import java.lang.System;
import java.lang.Math;
import java.lang.String;
import java.util.StringTokenizer;
import java.util.List;
import java.util.ArrayList;
// ============== end of my import
public class crosscorrelation {

    public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, DoubleWritable, IntWritable> {
      private final static IntWritable one = new IntWritable(1);
      //private Text word = new Text();
      DoubleWritable distance = new DoubleWritable();
      
      // helper function for logarithmic calculation
      // @return double
      public double log(double angle, double logBase){
      	double result = (Math.log(angle)/Math.log(logBase));
		return result;
	  } // end log
		
      public void map(LongWritable key, Text value, OutputCollector<DoubleWritable, IntWritable> output, Reporter reporter) throws IOException {
		// ======================================================== my lines
		// ========================================= solution No.1 
		/*
		 *	seperate files with special character "!"
		 *	not good, as file itself may contain "!"
		 *	improved-1st version: separate with a pair of sepcial character "!,!"
		 *	improved-2nd version: add a key in front of each value, 
		 *						indicating which input file the value belongs to
		 */
		
		
	// improved-2nd version
		
	// hold input values from all files
	ArrayList<String> list = new ArrayList<String>();
	// each counter records number of element in one file
	// count denotes where we are in the counter array 
	int[] counter = new int[10];
	// read the dataset, convert to array
	String line = value.toString();
	// get key-value pairs, i.e. if(token[0]==)1(key) then token[1] belong to file001, etc.
	String[] token = line.split(" ");
        
        // format of input file: key(space)value(space)value(space)key(space)...
        // i.e. a key and two values, all separated by a single space
        for(int i=0; i<token.length-3; i+=3){
		if(token[i].equals("1")){
	    	    	list.add(token[i+1]);
	    	    	list.add(token[i+2]);
	    	    	counter[0]+=2;
		}
		else if(token[i].equals("2")){
			list.add(token[i+1]);
			list.add(token[i+2]);
			counter[1]+=2;
		}
        }
        
        //============================== bining
		// if word belongs to a certain bin, then change that word to this bin's key
		// i.e. all members of the same bin has same key for reducer to count
		//String number = value.toString(); // convert Text to String
		//double angle = Double.parseDouble(number); // convert String to Double
		double base = 10; // base of log
		double minBin = 10; // smaller than min bin angle, have same key
		double maxBin = 500; // bigger than max bin angle, have same key
		int binCount = 10; // number of bins
		double deltaBin = (log(maxBin,base)-log(minBin,base))/(binCount); // difference between adjacent bins in log space
		double[] bins = new double[binCount+1]; // array of all bins
		for(int i=0; i<binCount+1; i++){
			// convert from log scale to normal scale
			bins[i] = Math.pow(base,1+i*deltaBin);
		}
		//============================ end of bin implementation
		
		for(int i=0; i<counter[0]; i++){
			for(int j=counter[0]; j<counter[1]+counter[0]; j++){
				double temp001 = Double.parseDouble(list.get(i));
				double temp002 = Double.parseDouble(list.get(j));
				double d = Math.abs(temp001-temp002);
				// bining d
				if(d<minBin){
            				d = minBin;
        			}
				if(d>=maxBin){
            				d = maxBin;
        			}
				if(d>=minBin && d<maxBin){
            				double temp = d;
					for(int k=0; k<binCount; k++){
						if(temp>=bins[k]){
					    		d = bins[k]; // reset d to this bin
						}
		    			}
				}
				distance.set(d);
				output.collect(distance,one);
			}
		}	
		
      } // end class map
    } // end class Map

public static class Reduce extends MapReduceBase implements Reducer<DoubleWritable, IntWritable, DoubleWritable, IntWritable> {
	public void reduce(DoubleWritable key, Iterator<IntWritable> values, OutputCollector<DoubleWritable, IntWritable> output, Reporter reporter) throws IOException {
		int sum = 0;
		while(values.hasNext()){
			sum += values.next().get();
		}
    		output.collect(key, new IntWritable(sum));
    	}
    }

    public static void main(String[] args) throws Exception {
	JobConf conf = new JobConf(crosscorrelation.class);
	conf.setJobName("crosscorrelation");

	conf.setOutputKeyClass(DoubleWritable.class);
	conf.setOutputValueClass(IntWritable.class);

	conf.setMapperClass(Map.class);
	conf.setCombinerClass(Reduce.class);
	conf.setReducerClass(Reduce.class);

	  //conf.setInputFormat(KeyValueTextInputFormat.class);	// key is Text type, for solution 2
	conf.setInputFormat(TextInputFormat.class);
	conf.setOutputFormat(TextOutputFormat.class);

	FileInputFormat.setInputPaths(conf, new Path(args[0]));
	FileOutputFormat.setOutputPath(conf, new Path(args[1]));

	JobClient.runJob(conf);
    }

}
