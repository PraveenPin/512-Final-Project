/*
Project Group : 12
Gunjan Singh (gs896)
Meghna Tumkur Narendra (mt1080)
Praveen Pinjala (pp813)
Shikha Vyaghra (sv629)
 */

package com.example;

import java.util.Arrays;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

public class Wordcount extends Configured implements Tool {

	private static Logger log = Logger.getLogger(Wordcount.class);

	public static void main(String[] args) throws Exception {
		try {
			int res = ToolRunner.run(new Configuration(), (Tool) new Wordcount(), args);
			System.exit(res);
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(255);
		}
	}

	public int run(String[] args) throws Exception {
        //Instantiate a configuration
		Configuration configuration = new Configuration();

        //Instantiate a job
	    Job job = Job.getInstance(configuration, "Your job name");

        //Check that user should provide both input file and output directory 
        if (args.length < 2) {
			log.warn("Please provide input file and output directory to run this job"+ job.getJar());
			return 1;
		}
		
        //Set job parameters
	    job.setJarByClass(Wordcount.class);
		log.info("job " + job.getJobName() + " [" + job.getJar()
				+ "] received following arguments: "
				+ Arrays.toString(args));

		job.setMapperClass(WordcountMapper.class);
		log.info("Mapper class is " + job.getMapperClass());
		log.info("Mapper output key class is " + job.getMapOutputKeyClass());
		log.info("Mapper output value class is " + job.getMapOutputValueClass());

		job.setReducerClass(WordcountReducer.class);
		log.info("Reducer class is " + job.getReducerClass());

		job.setCombinerClass(WordcountReducer.class);
		log.info("Combiner class is " + job.getCombinerClass());

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		log.info("Output key class is " + job.getOutputKeyClass());
		log.info("Output value class is " + job.getOutputValueClass());

		job.setInputFormatClass(TextInputFormat.class);
		log.info("Input format class is " + job.getInputFormatClass());

		job.setOutputFormatClass(TextOutputFormat.class);
		log.info("output format class is " + job.getOutputFormatClass());

        //Extract input file
		Path inputFile = new Path(args[0]);
		log.info("input path "+ inputFile);
		FileInputFormat.setInputPaths(job, inputFile);

        //Extract output directory, delete it if already exists
		Path outputDirectory = new Path(args[1]);
        FileSystem fileSystem = FileSystem.get(configuration);
		if (fileSystem.exists(outputDirectory))
			fileSystem.delete(outputDirectory, true);

		log.info("output path "+ outputDirectory);
		FileOutputFormat.setOutputPath(job, outputDirectory);

		job.waitForCompletion(true);
		return 0;
	}
}
