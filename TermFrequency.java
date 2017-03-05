// Developed by Nithin Venugopal
// nvenugo2@uncc.edu
// UNCC ID: 800966213

package org.myorg;

import java.io.IOException;
import java.util.regex.Pattern;
import java.lang.Math;
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


public class TermFrequency extends Configured implements Tool {

   private static final Logger LOG = Logger .getLogger( TermFrequency.class);

   public static void main( String[] args) throws  Exception {
      int res  = ToolRunner .run( new TermFrequency(), args);  // Run with toolrunner
      System .exit(res);
   }

   public int run( String[] args) throws  Exception {
      Job job  = Job .getInstance(getConf(), " wordcount ");  //Set the job configuration
      job.setJarByClass( this .getClass());   //Set the jar class 

      FileInputFormat.addInputPaths(job,  args[0]);   //Set input path from first argument
      FileOutputFormat.setOutputPath(job,  new Path(args[ 1]));  //Set output path from second argument
      job.setMapperClass( Map .class);  //Define mapper class
      job.setReducerClass( Reduce .class);    //Define reducer class
      job.setOutputKeyClass( Text .class);  // Set expected output type of key
      job.setOutputValueClass( DoubleWritable .class);  // Set expected output type of value

      return job.waitForCompletion( true)  ? 0 : 1;
   }
   //Map start from here. Here input will be Longwritable and text. Output is text and DoubleWritable
   public static class Map extends Mapper<LongWritable ,  Text ,  Text ,  DoubleWritable > {
      private final static DoubleWritable one  = new DoubleWritable( 1);   //used to put values in the intermediate file for each word
      private Text word  = new Text();

      private static final Pattern WORD_BOUNDARY = Pattern .compile("\\s*\\b\\s*");   //Regular expression used for splitting


      public void map( LongWritable offset, Text lineText,  Context context)
        throws  IOException,  InterruptedException {

         String line  = lineText.toString();  //Convert to string
         Text currentword  = new Text();
	
 	FileSplit fileSplit = (FileSplit)context.getInputSplit();  //Get the file split for the name
	String filename = fileSplit.getPath().getName();  //get the filename

         for ( String word  : WORD_BOUNDARY .split(line)) {   //use regular expression to split
            if (word.isEmpty()) {
               continue;
            }
	    word = word.toLowerCase();	//Convert all string to lower case
            currentword  = new Text(word);
	    Text filestring = new Text(currentword + "#####" +filename);  //Add a delimiter, concat file name and and the rest string
            context.write(filestring,one); //Write to intermediate file. here now one is doublewritable instead of intwritable earlier
         }
      }
   }

//Reducer starts from here. Takes Text, DoubleWritable as input and outputs Text and DoubleWritable
   public static class Reduce extends Reducer<Text ,  DoubleWritable ,  Text ,  DoubleWritable > {
      @Override 
      public void reduce( Text word,  Iterable<DoubleWritable > counts,  Context context)
         throws IOException,  InterruptedException {
         int sum  = 0;
   //sum each word from the list using iterator
         for ( DoubleWritable count  : counts) {
            sum  += count.get();
         }

// take the log of that sum 
double tf = Math.log10(sum);

//Write the output file with adding 1 to the above result to get the termfrequency
         context.write(word, new DoubleWritable(1.0+tf));
      }
   }
}
