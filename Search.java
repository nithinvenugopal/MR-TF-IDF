// Developed by Nithin Venugopal
// nvenugo2@uncc.edu
// UNCC ID: 800966213

package org.myorg;

import java.io.IOException;
import java.util.regex.Pattern;
import java.lang.Math;
import java.util.ArrayList;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.*;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;


public class Search extends Configured implements Tool {

	private static final Logger LOG = Logger.getLogger(Search.class);
	private static final String arguments = "arg";   //Assigning global variables

	public static void main(String[] args) throws Exception {


		int res = ToolRunner.run(new Search(), args);  //Calling the run command function
		System.exit(res);
	}

	public int run(String[] args) throws Exception {
		Job job = Job.getInstance(getConf(), " wordcount ");  //Creating the job for further configuration
		job.setJarByClass(this.getClass());  //Setting the Jar class


		String[] nargs = new String[args.length - 2]; // Reading the arguments to the string from 3rd argument and putting it to new array
		for(int i = 2; i < args.length; i++){
			nargs[i-2] = args[i];
		}

		//setting the number of arguments to pass to all the mapper jobs
		job.getConfiguration().setStrings(arguments,nargs);

		FileInputFormat.addInputPaths(job, args[0]);  //First argument is input path
		FileOutputFormat.setOutputPath(job, new Path(args[1]));  //Second argument will be the output path


		job.setMapperClass(Map.class); // Setting the mapper class
		job.setReducerClass(Reduce.class);  // Setting the reducer class
		job.setOutputKeyClass(Text.class);  // Sets the output key class as Text
		job.setOutputValueClass(DoubleWritable.class); //Set the output values class as DoubleWritable

		return job.waitForCompletion(true) ? 0 : 1;

	}


	//Following code implements the mapper for the search.
	public static class Map extends Mapper<LongWritable, Text, Text, DoubleWritable> {
		private final static DoubleWritable one = new DoubleWritable(1);
		private Text word = new Text();


		// This statement is used for regular expression to pick each word
		private static final Pattern WORD_BOUNDARY = Pattern.compile("\\s*\\b\\s*");

		public void map(LongWritable offset, Text lineText, Context context)
				throws IOException, InterruptedException {

			String line = lineText.toString();
			ArrayList<Text> arrayarguments = new ArrayList<>();
			double tfidflist= 0;

			if (line.isEmpty()) {
				return;
			}
			line = line.toLowerCase();

			// Each line is split with the delimitter
			String[] splline = line.split("#####");
			if (splline.length < 2) {
				return;
			}
			// Splitted parts are assigned into the variable
			Text Word = new Text(splline[0]);
			Text wordDetails = new Text(splline[1]);

			// Get all the arguments and put it into array from global variable
			String[] noargs = context.getConfiguration().getStrings(arguments);

			//From the string, each argument is put into the array list
			for (int i = 0; i < noargs.length; i++) {
				arrayarguments.add(new Text(noargs[i]));
			}

			// Each word from the file read is compared to check whether it is present in the arraylist
			if (!arrayarguments.contains(Word)) {
				return;
			}

			// The second part of the line which contains file name and TF-IDf score is splitted using tab as delimitter
			String parts[] = wordDetails.toString().split("\t");
			Word = new Text(parts[0]);

			tfidflist = Double.parseDouble(parts[1]);
			// Text file and TF-IDf score is put into intermediate file. The key will be the text file and value will be doubleWritable
			// Value will the TF-IDf score
			context.write(Word, new DoubleWritable(tfidflist));




		}
	}

	// Reducer class starts from here. Reads Text and DoubleWritable as input and outpus key as text and value as DoubleWritable
	public static class Reduce extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
		@Override
		public void reduce(Text word, Iterable<DoubleWritable> counts, Context context)
				throws IOException, InterruptedException {
			double sum = 0;

//The following will add the list of all the values from the same text file. So the values will be iterable and we use for loop to access.

			for (DoubleWritable count : counts) {
				sum += count.get();
				//We add list of all TF-IDF score here for each key
			}

			//ouput the text file and search score for the given search term given through argument
			context.write(word, new DoubleWritable(sum));
		}
	}
}
