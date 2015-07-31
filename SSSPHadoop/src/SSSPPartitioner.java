import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class SSSPPartitioner extends Partitioner<IntWritable, Text>{
	@Override
	public int getPartition(IntWritable key, Text value, int noOfReducers) {
		//Having multiple nodes in sorted order of their nodeId makes efficient load balancing easier by
		//returning the nodeId modulo number of reducer. Thus the follwing partitioner was return kept this idea
		// in mind.
		//Get nodeId
		int nodeId = key.get();
		//Return the reducer id
		return nodeId%noOfReducers;
	}
 }// End of Partitioner