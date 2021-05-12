package com.example;

import java.io.IOException;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;

public class Step2Reducer extends Reducer<Text, Text, Text, Text> 
{
	
	private float dampingFactor;
	
	@Override
	protected void setup(Context context)
	{
		dampingFactor = context.getConfiguration().getFloat("dampingFactor", 0.85f);
	}

	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException 
	{
		String outlinks = "";
		float totalRank = 0;
		
		for (Text text : values)
		{
			String val = text.toString();
			
			if (val.startsWith("["))
			{
				outlinks = val.substring(1);
				continue;
			}
			else
			{
				totalRank += Float.parseFloat(val);
			}
		}
		
		totalRank = (1 - dampingFactor) + (dampingFactor * totalRank);
		context.write(key, new Text(Float.toString(totalRank) + "\t" + outlinks));
	}
	
}
