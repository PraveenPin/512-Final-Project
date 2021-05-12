/*
Project Group : 12
Gunjan Singh (gs896)
Meghna Tumkur Narendra (mt1080)
Praveen Pinjala (pp813)
Shikha Vyaghra (sv629)
 */

package com.example;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WordcountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

	@Override
	protected void map(LongWritable offset, Text line, Context context)
			throws IOException, InterruptedException {

        // Split a line in to words and output (word, 1)
		for (String word : line.toString().split(" ")) {
			context.write(new Text(word), new IntWritable(1));
		}
	}
}
