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
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class WordcountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

	@Override
	protected void reduce(Text key, Iterable<IntWritable> values, Context context)
			throws IOException, InterruptedException {
        //Sum up counts for the zero
		int count = 0;
		for (IntWritable current : values) {
			count += current.get();
		}

        // output (word, count)
		context.write(key, new IntWritable(count));
	}

}
