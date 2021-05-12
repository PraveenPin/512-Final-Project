package com.example;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;

public class PageRank {
	
	
	public static void main(String[] args) throws Exception
	{
		if (args.length != 8)
		{
			System.out.println("Arguments are not enough, expected 8 (inputpath, outputpath, datapath, df, maxruns, mindiff, deleteoutput, showresults), found" + args.length);
			System.exit(1);
		}

		int maxRuns = Integer.parseInt(args[4]);
		float dampingFactor = Float.parseFloat(args[3]);
		float mindiff = Float.parseFloat(args[5]);

        //Delete output folder if already exists
		FileSystem filesystem = FileSystem.get(new Configuration());
		if (Boolean.parseBoolean(args[6]))
		{
			Path outputPath = new Path(args[1]);
			if (filesystem.exists(outputPath))
			{
				System.out.println("Deleting /output..");
				filesystem.delete(outputPath, true);
			}
		}
		
		// Step 1
		boolean success = step1(args[0], args[1] + "/ranks0");
		
		// Step 2
		HashMap<Integer, Float> lastRanks = getRanks(filesystem, args[1] + "/ranks0");
		for (int i = 0; i < maxRuns; i++) 
		{
			success = success && step2(args[1] + "/ranks" + i, args[1] + "/ranks" + (i + 1), dampingFactor);
			
			// Calculate diff of ranks
			HashMap<Integer, Float> newRanks = getRanks(filesystem, args[1] + "/ranks" + (i + 1));
			float diff = calculateDiff(lastRanks, newRanks);
			System.out.println("Run #" + (i + 1) + " finished (score diff: " + diff + ").");
			
			// If the diff is lower than the mindiff we stop the iterations Otherwise continue
			if (diff < mindiff)
				break; 
			else
				lastRanks = newRanks;
		}
		
		// Step 3
		success = success && step3(args[1] + "/ranks" + maxRuns, args[1] + "/ranking", args[2]);
		
		// Show results if asked
		if (Boolean.parseBoolean(args[7]))
		{
			showResults(filesystem, args[1] + "/ranking");
		}
		
		System.exit(success ? 0 : 1);
	}
	
	private static boolean step1(String input, String output) throws Exception
	{
        // Instantiate configuration fro step 1
		Configuration conf = new Configuration();
		conf.set("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false");

		System.out.println("Step 1..");

        //Instantiate job for step 1
		Job job = Job.getInstance(conf, "Step 1");
		job.setJarByClass(PageRank.class);
		
        //Set params of job
		job.setMapperClass(Step1Mapper.class);
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setReducerClass(Step1Reducer.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(Text.class);
		
        //Set input/output path 
		FileInputFormat.addInputPath(job, new Path(input));
		FileOutputFormat.setOutputPath(job, new Path(output));
		
		return job.waitForCompletion(true);
	}
	
	private static boolean step2(String input, String output, float dampingFactor) throws Exception
	{
        //Instantiate configuration for step 2
		Configuration conf = new Configuration();
		conf.set("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false");
		conf.setFloat("df", dampingFactor);
		
		System.out.println("Step 2..");
        
        // Instantiate job for step 2
		Job job = Job.getInstance(conf, "Step 2");

        //Set job params 
		job.setJarByClass(PageRank.class);
		job.setMapperClass(Step2Mapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setReducerClass(Step2Reducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
        //Set input/output
		FileInputFormat.addInputPath(job, new Path(input));
		FileOutputFormat.setOutputPath(job, new Path(output));
		
		return job.waitForCompletion(true);
	}

	private static boolean step3(String input, String output, String urlsPath) throws Exception
	{
        // Instantiate configuration for step3
		Configuration conf = new Configuration();
		conf.set("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false");
		conf.set("urls_path", urlsPath);
		
		System.out.println("Step 3..");

        // Instantiate job for step 3
		Job job = Job.getInstance(conf, "Step 3");

        //Set job params for step 3
		job.setJarByClass(PageRank.class);
		job.setSortComparatorClass(SortFloatComparator.class);
		job.setMapperClass(Step3Mapper.class);
		job.setMapOutputKeyClass(FloatWritable.class);
		job.setMapOutputValueClass(Text.class);
		
        //Set input/output
		FileInputFormat.addInputPath(job, new Path(input));
		FileOutputFormat.setOutputPath(job, new Path(output));
		
		return job.waitForCompletion(true);
	}
	
	private static void showResults(FileSystem fs, String dir) throws Exception
	{
		Path path = new Path(dir + "/part-r-00000");
		if (!fs.exists(path))
		{
			System.out.println("The file part-r-00000 doesn't exist.");
			return;
		}
		
		BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(path)));
		String line;
		while ((line = br.readLine()) != null)
		{
			System.out.println(line);
		}
	}
	
	private static HashMap<Integer, Float> getRanks(FileSystem fs, String dir) throws Exception
	{
		Path path = new Path(dir + "/part-r-00000");
		if (!fs.exists(path))
			throw new Exception("The file part-r-00000 doesn't exist.");
		
		BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(path)));
		String line;
		HashMap<Integer, Float> ranks = new HashMap<Integer, Float>();
		
		while ((line = br.readLine()) != null)
		{
			String[] split = line.split("\t");
			ranks.put(Integer.parseInt(split[0]), Float.parseFloat(split[1]));
		}
		
		return ranks;
	}
	
	private static float calculateDiff(HashMap<Integer, Float> lastRanks, HashMap<Integer, Float> newRanks)
	{
		float diff = 0;
		
		for (int key : newRanks.keySet())
		{
			float lri = lastRanks.containsKey(key) ? lastRanks.get(key) : 0;
			diff += Math.abs(newRanks.get(key) - lri);
		}
		
		return diff;
	}
	
}
