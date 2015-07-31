import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * @author Vishal Doshi
 *
 */
public class SSSPMapper extends Mapper<LongWritable, Text, IntWritable, Text>
{
  	@Override
  	//Process every node that is gray and explore its neighbors
  	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
  		Node child;
  		String nodeProperties = value.toString();
  		//Create a Node
  		Node node = new Node(nodeProperties);
  		if(node.getColor() == Node.Color.GRAY ){ // 'G'
  			int distance = node.getDistance();
  	       	int idParent = node.getId();
  	       	//Explore their neighbors
  	       	ArrayList<Integer> neigh = node.getNeighbors();
  	       	for(int i: neigh){
  	       		child = new Node(i,distance+1,idParent); // Node(IDP)
  	       		//Writing the explored neighbors
  	       		context.write(new IntWritable(child.getId()),new Text(child.toString()));
  	       	}
  	       	//Change the explored node to Black color
  	       	node.setColor(Node.Color.BLACK);
  	    }
  		//Black, White and Gray => Black nodes Written
  		context.write(new IntWritable(node.getId()), new Text(node.toString()));
  	}// End of map()	         
  }// End of Mapper   