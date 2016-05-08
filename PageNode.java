import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.Text;



public class PageNode {

	private String id;
	private List<String> edges = new ArrayList<String>();
	private double pageValue;
	//private double last_pagevalue;
	public PageNode(){
		pageValue=1/PageRank.lines;
		//dif=Double.MAX_VALUE;
	}
	public PageNode(String nodeInfo){
		// dif=Double.MAX_VALUE;
		 String[] inputLine = nodeInfo.split("\t");
		 String key = "", value = "";
		 key = inputLine[0].replaceAll("[^0-9.]", ""); // node id
         value = inputLine[1];
         this.id=key;
         //System.out.println(key);
         String[] tokens=value.split("\\|");
         for (String s : tokens[0].split(",")) {
             if (s.length() > 0) {
                 edges.add(s);
             }
         }
         if (tokens[1].equals("Double.MAX_VALUE")) {
             this.pageValue = 1/((double)PageRank.lines);
             //System.out.println("page lines "+PageRank.lines);
         } else {
             this.pageValue = Double.parseDouble(tokens[1]);
         }
        /* if (tokens[2].equals("Double.MAX_VALUE")) {
             this.last_pagevalue = 1/((double)PageRank.lines);
             //System.out.println("page lines "+PageRank.lines);
         } else {
             this.last_pagevalue = Double.parseDouble(tokens[1]);
         }*/
         
	}
	public Text getNodeInfo() {
        StringBuffer s = new StringBuffer();    
        // forms the list of adjacent nodes by separating them using ','
        try {
            for (String v : edges) {
                s.append(v).append(",");
            }
        } catch (NullPointerException e) {
            e.printStackTrace();
            System.exit(1);
        }
        
 
        // after the list of edges, append '|'
        /*if(s.charAt(s.length()-1)==','){
        	System.out.println(s);
        	s.deleteCharAt(s.length()-1);
        	System.out.println(s);
        }*/
        //s.setLength(s.length() - 1);
        s.append("|");
 
        // append the minimum distance between the current distance and
        // Integer.Max_VALUE
        if (this.pageValue < Double.MAX_VALUE) {
            s.append(String.valueOf(this.pageValue));
        } else {
            s.append("Double.MAX_VALUE");
        }
        //s.append("|");
        /*if (this.last_pagevalue < Double.MAX_VALUE) {
            s.append(String.valueOf(this.pageValue));
        } else {
            s.append("Double.MAX_VALUE");
        }*/
        return new Text(s.toString());
    }
	public String getId() {
		return this.id;
	}

	public double getpageValue() {
		return this.pageValue;
	}

	public void setId(String id) {
		this.id = id;
	}

	public void setPageValue(double distance) {
		this.pageValue = distance;
	}

	public List<String> getEdges() {
		return this.edges;
	}

	public void setEdges(List<String> edges) {
		this.edges = edges;
	}
	/*public void setDif(double last){
		this.last_pagevalue=last;
	}
	public double getDif(){
		return this.last_pagevalue;
	}*/

}
