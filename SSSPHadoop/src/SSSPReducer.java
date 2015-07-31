import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * @author Vishal Doshi
 *
 */
public class  SSSPReducer extends Reducer<IntWritable, Text, IntWritable, Text>
{
	@Override	 
	public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
    	Node dummy = new Node(key.get());
    	
	    //Iterate through all the values and compare every node with same id to get it's darkest color,
	    //minimum distance from source and the parent
	   	for (Text value: values){	
	   		Node valNode = new Node(key.get()+"\t"+value.toString());     
	    	
	   		//Fetching the list of neighbors
	    	if (valNode.getNeighbors().size() > 0){
	    		dummy.setNeighbors(valNode.getNeighbors());
	    	}
	    	//Finding minimum distance and also the parent
	    	int distance= valNode.getDistance();
	    	if( distance < dummy.getDistance()){
	    		dummy.setDistance(distance);
	    		dummy.setParent(valNode.getParent());
	    	}
	    			
	    	//Finding dark color
	    	if(dummy.getColor().ordinal() < valNode.getColor().ordinal()){
	    		
	    		//Change of White => Black cannot occur unless the node is disconnected from the source( i.e. no path from source)
	    		// where the distance would be : Integer.MAX_VALUE and parent will be : null
	    		
	    		// If Color changes from White to Gray, increment the counter as one more MapRed Iteration needs to be performed.
	    		if(dummy.getColor() == Node.Color.WHITE && valNode.getColor() == Node.Color.GRAY){
	    			//Increment by 1.
	    			context.getCounter(SSSPHadoop.MoreIterations.NUMBEROFITERATIONS).increment(1);	
	    		}
	    		
	    		// If Color changes from Gray to Black, decrement the counter as one less MapRed Iteration to be performed.
	    		else if(dummy.getColor()== Node.Color.GRAY && valNode.getColor() == Node.Color.BLACK){
	    			//Decrement by 1 ( increment by -1).
	    			context.getCounter(SSSPHadoop.MoreIterations.NUMBEROFITERATIONS).increment(-1);
	    		}
	    		dummy.setColor(valNode.getColor());
	    	}
	    }	// End for
	    // Write the values to the file
	    context.write(new IntWritable(dummy.getId()),new Text(dummy.toString()));
	}
} // End of Reducer

