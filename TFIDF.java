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


public class TFIDF extends Configured implements Tool {

   private static final Logger LOG = Logger .getLogger( TFIDF.class);  //Set the logger class
private static final String COUNT = "filecount";    //Declare global variable to store the file count

   public static void main( String[] args) throws  Exception {

//We will call TermFrequency using the toolrunner which writes the output to it's folder
   int res1  = ToolRunner .run( new TermFrequency(), args);

// This toolrunner calls the TFIDF function present here
      int res2  = ToolRunner .run( new TFIDF(), args);
      System .exit(res1);
      System .exit(res2);
   }

   public int run( String[] args) throws  Exception {


// set the file system configuration
FileSystem files = FileSystem.get(getConf());
Path pat = new Path(args[0]);  //set the input path to get the input file count
ContentSummary CS = files.getContentSummary(pat);  //get the contect summary of all the files in the path
long filecount = CS.getFileCount();  //get the filecount in the input path
getConf().set(COUNT, "" + filecount); //set the file count

Job job2  = Job .getInstance(getConf(), " TFIDF "); //set job for further configuration and running 
      job2.setJarByClass( this .getClass()); //set jar class

      FileInputFormat.addInputPaths(job2,  args[1]);  //input path to the tfidf function which will be the ouput of termfrequency aswell
      FileOutputFormat.setOutputPath(job2,  new Path(args[2])); //output path of tfidf reducer

//Set mapper class
      job2.setMapperClass( Map2 .class);

//Set reducer class
      job2.setReducerClass( Reduce2 .class);      
//set output key class from mapper
      job2.setOutputKeyClass( Text .class);

//set value type of the output from mapper 
      job2.setOutputValueClass( Text .class);
return job2.waitForCompletion( true)  ? 0 : 1;

   }
   

// Mapper starts from here
public static class Map2 extends Mapper<LongWritable ,  Text,  Text , Text> {
   
      private static final Pattern WORD_BOUNDARY = Pattern .compile("\\s*\\b\\s*");    //set the regular expression

      public void map( LongWritable offset, Text lineText,  Context context)
        throws  IOException,  InterruptedException {

         String line  = lineText.toString();  //Convert to string and read using line
      
            if (line.isEmpty()) {   //If the line is empty then return and read next line
               return;
            }
	   line = line.toLowerCase();	//convert to lower case
            String[] splline = line.split("#####");   //split with #####
		if(splline.length < 2){    // just to take care of array out of bound
				return;
			}
	
	 Text words= new Text (splline[0]);  //first split will have words as the key
	 Text wordsummary= new Text (splline[1]);  //second split will have file name and term frequency
         context.write(words, wordsummary);   //Write both to intermediate file
         
      }
   }

//Reducer starts from here. It takes text, text as input from mapper and outputs text and doublewritable which is text and tfidf score
   public static class Reduce2 extends Reducer<Text ,  Text ,  Text ,  DoubleWritable > {
      @Override 
      public void reduce( Text word,  Iterable<Text> counts,  Context context)
         throws IOException,  InterruptedException {
         double sum  = 0;
          double tfidf = 0;

//Get the global file count value
long fcount = context.getConfiguration().getLong(COUNT, 1);
ArrayList<Text> wordinfiles = new ArrayList<>();   //Consider an array list to store second part of the string
	 for ( Text count  : counts) {    //run for each string
            sum++;
            wordinfiles.add(new Text(count.toString()));   //add it the array list
         }
	for (Text having : wordinfiles)   //We will run this for each string
{
String[] parts = having.toString().split("\t");   //split the file name and term frequency with the delimitter tab

double IDF = Math.log10(1.0 + (fcount/sum));   // Calculate the inverse document frequency
tfidf = Double.parseDouble(parts[1])*IDF;    // multiply idf and wordfrequency
context.write(new Text(word + "#####" + parts[0]), new DoubleWritable(tfidf));   //Write output to output path. add the word, delimitter, file name and then tfidf score

}

	
      }
   }
}
