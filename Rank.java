// Developed by Nithin Venugopal
// nvenugo2@uncc.edu
// UNCC ID: 800966213

package org.myorg;

import java.io.IOException;
import java.util.regex.Pattern;
import java.lang.*;
import java.util.HashMap;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.*;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;



public class Rank extends Configured implements Tool {

    private static final Logger LOG = Logger .getLogger(Rank.class);
    public static void main( String[] args) throws  Exception
    {
      int res  = ToolRunner .run( new Rank(), args);
      System .exit(res);
    }


   public int run( String[] args) throws  Exception {

     
 Job job  = Job .getInstance(getConf(), " Rank ");  //Get the job configuration

 job.setJarByClass( this .getClass());    //set the jar by class
 FileInputFormat.addInputPaths(job,  args[0]);    //set the first argument as the input path
 FileOutputFormat.setOutputPath(job, new Path(args[1])); // set the second argument as the output path
 job.setMapperClass( Map .class);   //set the mapper class
 job.setReducerClass( Reduce .class);  //set the reducer class
 job.setMapOutputKeyClass(DoubleWritable.class);  //set the output key as DoubleWritable for the map
 job.setMapOutputValueClass(Text.class);  //Set the output value as Text for the map
 job.setSortComparatorClass(Mysorter.class);  // Using comparator class for sorting
 job.setOutputKeyClass( Text .class);   // set the output key as Text
 job.setOutputValueClass(DoubleWritable .class); //set the output key as DoubleWritable

     return job.waitForCompletion( true)  ? 0 : 1;

   }


   // Using natural key grouping comparator. Referred from vangjee blog

public static class Mysorter extends WritableComparator {
    protected Mysorter() {
          super(DoubleWritable.class, true);
    }
 // relevant rows within a partition of data are sent to a single reducer
    @SuppressWarnings("rawtypes")
    @Override
    public int compare(WritableComparable w1, WritableComparable w2) {
        DoubleWritable k1 = (DoubleWritable) w1;
        DoubleWritable k2 = (DoubleWritable) w2;
        return -1 * k1.compareTo(k2);
    }
}

// Mapper start from here. Read the input from the file as Text.
   public static class Map extends Mapper<LongWritable, Text,  DoubleWritable,  Text > {
    
      private static final Pattern WORD_BOUNDARY = Pattern .compile("\\s*\\b*\\s*");

      public void map(LongWritable offset, Text lineText, Context context)
        throws  IOException,  InterruptedException {

          //Each line of the text is taken
         String line  = lineText.toString();

         // It is split using tab delimitter
                    String[] part=line.split("\t");
if(part.length>=0)   //making sure for array out of bound
{

    // Here the search score goes as key and file name as value to the intermediate file
context.write(new DoubleWritable(Double.parseDouble(part[1])),new Text(part[0]));
}

      }
   }


   //Reducer start from here. Reads DoubleWritable as input key and Text as input value. Outputs Text and DoubleWritable for key and value
   public static class Reduce extends Reducer<DoubleWritable ,  Text ,  Text ,  DoubleWritable > {
      @Override 
      public void reduce(DoubleWritable score,  Iterable<Text> files,  Context context)
         throws IOException,  InterruptedException {

       for(Text file:files)
{
    // Writing the sorted output file to output file
context.write(file,score);
}

         
      }
   }
}



