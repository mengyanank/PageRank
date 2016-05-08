import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class PageMapper extends Mapper<Object, Text, Text, Text>{
		public void map(Object key,Text value,Context context,PageNode inNode) throws IOException, InterruptedException
		{
			/*String nodeInfo=value.toString();
			String[] inputLine =nodeInfo.split("\t");
			int id=Integer.parseInt(inputLine[0]);
			double pageValue=0.0;
			String[] tokens=inputLine[1].split("|");
			if(tokens[1].equals("Integer.MAX_VALUE"))
			{
				pageValue=0.2;
			}
			else
				pageValue= Double.parseDouble(tokens[1]);
			String[] outIDs=tokens[0].split(",");
			int total_outDegree=outIDs.length;
			double ave_pageValue=pageValue/total_outDegree;
			for (String s : outIDs) {
                  int outID=Integer.parseInt(s);
                  context.write(new IntWritable(outID), new DoubleWritable(ave_pageValue));
                }*/
			int N=inNode.getEdges().size();
			double ave_pv=0.0;
			//System.out.println(N);
			if(N!=0)
			 ave_pv=inNode.getpageValue()/N;
			for (String neighbor : inNode.getEdges())
			{
				PageNode adjacentNode = new PageNode();
				adjacentNode.setId(neighbor);
				adjacentNode.setPageValue(ave_pv);
				context.write(new Text(adjacentNode.getId()), adjacentNode.getNodeInfo());
			}
			String tw=inNode.getNodeInfo().toString();
			tw+="|itself";
			//context.write(new Text(inNode.getId()), inNode.getNodeInfo());
			context.write(new Text(inNode.getId()), new Text(tw));
         }	
	}
