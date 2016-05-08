import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;



public class PageRank {

	//public static int lines=6760660;
	public static int lines=31;
	static HashMap hmap=new HashMap();
	static enum MoreIterations {
		numberOfIterations
	}
	
public static class PageMapperSSSP extends PageMapper {

		
		public void map(Object key, Text value, Context context)
		throws IOException, InterruptedException {
		
			PageNode inNode = new PageNode(value.toString());
			//System.out.println(value.toString());
			//calls the map method of the super class SearchMapper
			super.map(key, value, context, inNode);

		}
	}
public static class PageReducerSSSP extends PageReducer{


	//the parameters are the types of the input key, the values associated with the key and the Context object through which the Reducer communicates with the Hadoop framework

	
	public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {

		   //create a new out node and set its values
		PageNode outNode = new PageNode();
		System.out.println("reduce");
			//call the reduce method of SearchReducer class 
			//outNode = super.reduce(key, values, context, outNode);										
            double dif= super.reduce(key, values, context, outNode);
			//if the color of the node is gray, the execution has to continue, this is done by incrementing the counter
			//System.out.println("difference "+dif);
            if (Math.abs(dif)>0.001)
            {
				context.getCounter(MoreIterations.numberOfIterations).increment(1L);
				//System.out.println("dif "+dif);
            }
	}
}
    public static class LineMapper extends Mapper<Object, Text, Text, Text>{
    	public static Text l=new Text();
    	public void map(Object key, Text value, Context context)
    			throws IOException, InterruptedException {
    		l=value;   		
    		//lines+=1;
    		String s=l.toString();
    		//System.out.println(s);
    		String[] str=s.split(",");
    		String to=str[1].replaceAll("[^0-9]", "");
    		String from=str[2].replaceAll("[^0-9]", "");
    		//if(Integer.parseInt(from)<=100&&Integer.parseInt(to)<=100){
    		   //System.out.println("from "+from+" to "+to);
    		context.write(new Text(to),new Text(from));
    		if(!hmap.containsKey(from))
    		{
    			context.write(new Text(from), new Text("N"));
    			hmap.put(from, 1);
    		}  		
    		//}
   		//context.write(new Text(to),new Text(from));
    	}
    }
    public static class LineReducer extends Reducer<Text, Text,Text,Text>{
    	public void reduce(Text key, Iterable<Text> values, Context context)
    			throws IOException, InterruptedException{
    		lines+=1;
    		//System.out.println("lines incresing: "+lines);
    		String s="";
    		for (Text val : values) {
    			if(!val.toString().equals("N"))
    			{
    			 s+=val.toString();
    			 s+=",";
    			}
    	      }
    		if(s.length()>0)
    		  s=s.substring(0,s.length()-1);
    		s+="|Double.MAX_VALUE";
    		String keyid="id:"+key.toString();
    		context.write(new Text(keyid),new Text(s) );
    	}
    }
	public static void main(String[] args) throws Exception
	{
		int iterationCount = 0; 
		Job job;		
		long terminationValue =1;
		
		System.out.println("input: "+args[0]);
		/*Job linejob;
		Configuration lineconf = new Configuration();
		linejob = Job.getInstance(lineconf, "LineCount");
		linejob.setJarByClass(PageRank.class);
		linejob.setMapperClass(LineMapper.class);
		linejob.setReducerClass(LineReducer.class);
		linejob.setOutputKeyClass(Text.class);
		linejob.setOutputValueClass(Text.class);
	    FileInputFormat.addInputPath(linejob, new Path(args[0]));
	    FileOutputFormat.setOutputPath(linejob, new Path(args[1]+"lineoutput"));
	    linejob.waitForCompletion(true);
	    System.out.println("number of lines after counting"+lines);*/
	    if(lines==0)
	    	System.exit(1);
		while(terminationValue >0){
			//System.out.println(terminationValue);
			Configuration conf = new Configuration();
		    job = Job.getInstance(conf, "PageRank");
		    String input, output;
		    if(iterationCount==0)
		    	input=args[0];
		    	//input=args[1]+"lineoutput";
		    else
		    	input=args[1] + iterationCount;
		    output=args[1] + (iterationCount + 1);
		    System.out.println(output);
		    job.setJarByClass(PageRank.class);
		    job.setMapperClass(PageMapperSSSP.class);
		    //job.setCombinerClass(PageReducerSSSP.class);
		    job.setReducerClass(PageReducerSSSP.class);
		    job.setOutputKeyClass(Text.class);
		    job.setOutputValueClass(Text.class);
		    FileInputFormat.addInputPath(job, new Path(input));
		    FileOutputFormat.setOutputPath(job, new Path(output));
		    job.waitForCompletion(true);
		    Counters jobCntrs = job.getCounters();
			terminationValue = jobCntrs.findCounter(MoreIterations.numberOfIterations).getValue();//if the counter's value is incremented in the reducer(s), then there are more GRAY nodes to process implying that the iteration has to be continued.
			//System.out.println(terminationValue);
			iterationCount++;
		}
		System.out.println("finished");
		 System.exit(0);
	}
}
