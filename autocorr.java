// Autocorrelation

package org.myorg;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.util.StringUtils;

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
	
public class logBinAngleCount {

    public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, DoubleWritable, IntWritable> {
      private final static IntWritable one = new IntWritable(1);
      //private Text word = new Text();
      DoubleWritable distance = new DoubleWritable();
      
      // helper function for logarithmic calculation
      // @return double
      public double log(double angle, double logBase){
      	double result = (Math.log(angle)/Math.log(logBase));
		return result;
	  }
		
      public void map(LongWritable key, Text value, OutputCollector<DoubleWritable, IntWritable> output, Reporter reporter) throws IOException {
		// ======================================================== my lines
		
		
		// =============================== new implements
		// ArrayList to hold input values
		ArrayList<Double> list = new ArrayList<Double>();
		// read the dataset, convert to array
		String line = value.toString();
		String[] token = line.split(" ");
		
        // array to hold 1st column in data set
        ArrayList<Double> list01 = new ArrayList<Double>();

        for (int i=0; i<token.length; i++) {
        	double curr = Double.parseDouble(token[i]); // convert String to Double
        	list.add(curr);
        	if (i%2!=0)
        		// convert degree to arcseconds
        		list01.add(3600*curr);
        	else continue;
        }
        
        //============================== bining
		//The correlation function should go from 2 arcseconds to 10 degrees (1 degree = 3600 arcsecond)
		double base = 10; // base of log
		double minBin = 2; // smaller than min bin angle, have same key
		double maxBin = 36000; // bigger than max bin angle, have same key
		int binCount = 30; // number of bins
		double deltaBin = (log(maxBin,base)-log(minBin,base))/(binCount); // difference between adjacent bins in log space
		double[] bins = new double[binCount+1]; // array of all bins
		for(int i=0; i<binCount+1; i++){
			// convert from log scale to normal scale
			bins[i] = Math.pow(base,1+i*deltaBin);
		}
		//============================ end of bin implementation
		
        int l = list01.size();
        for(int i=0; i<l; i++){
        	for(int j=i; j<l; j++){
        		if(i!=j){
	        		double me = list01.get(i);
    	    		double you = list01.get(j);
    	    		double d=Math.abs(me-you);
    	    		
    	    		//bining d
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
    	    	else continue;
        	}
        }

      }
    }

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
      JobConf conf = new JobConf(logBinAngleCount.class);
      conf.setJobName("autocorrelation");

      conf.setOutputKeyClass(DoubleWritable.class);
      conf.setOutputValueClass(IntWritable.class);

      conf.setMapperClass(Map.class);
      conf.setCombinerClass(Reduce.class);
      conf.setReducerClass(Reduce.class);

      conf.setInputFormat(TextInputFormat.class);
      conf.setOutputFormat(TextOutputFormat.class);

      FileInputFormat.setInputPaths(conf, new Path(args[0]));
      FileOutputFormat.setOutputPath(conf, new Path(args[1]));

      JobClient.runJob(conf);
    }
}

