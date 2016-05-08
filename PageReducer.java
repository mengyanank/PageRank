import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class PageReducer extends Reducer<Text, Text,Text,Text>
	{
		 /*public void reduce(IntWritable key, Iterable<DoubleWritable> values, Context context)
	                throws IOException, InterruptedException 
	     {
			   double sum=0,d=0.8,pageValue=0.0;
	           for (DoubleWritable value : values)
	           {
	             sum+=value.get();
	           }
	           pageValue=(1-d)/5+d*sum;
	           
	           context.write(key, new Text(String.valueOf(pageValue)));
	     }*/
		public double reduce(Text key, Iterable<Text> values, Context context,PageNode outNode)
                throws IOException, InterruptedException{
			outNode.setId(key.toString());
			double sum=0.0;
			double lastPV=0.0;
			double dif=0.0;
			for (Text value : values) {
                String vs=value.toString();
				PageNode inNode = new PageNode(key.toString() + "\t" + vs);

				// One (and only one) copy of the node will be the fully expanded version, which includes the list of adjacent nodes, in other cases, the mapper emits the ndoes with no adjacent nodes
				//In other words, when there are multiple values associated with the key (node Id), only one will 
				if (vs.split("\\|").length > 2) {
					outNode.setEdges(inNode.getEdges());
					lastPV=inNode.getpageValue();
				}
				else{
					sum+=inNode.getpageValue();
				}
				
			}
			//outNode.setDif(sum-lastPV);
			
			double tsum=sum*0.8+0.2/PageRank.lines;
			//System.out.println("page lines "+PageRank.lines);
			dif=tsum-lastPV;
			outNode.setPageValue(tsum);
			String keyid="id:"+key.toString();
			System.out.println(keyid);
			context.write(new Text(keyid), new Text(outNode.getNodeInfo()));
			//return outNode;
			/*if(Math.abs(dif)>0.001){
			System.out.println("dif : "+dif+" id: "+outNode.getId());
			
			}*/
			return dif;
		}
	}
