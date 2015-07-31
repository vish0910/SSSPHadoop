
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.ToolRunner;


/**
 * @author Vishal Doshi
 *
 */

public class SSSPHadoop extends Configured implements Tool
{
	
	static enum MoreIterations{
		NUMBEROFITERATIONS
		 }
    public int run(String[] args) throws Exception{
    	int iterationCount = 0;
    	long terminationValue=1;
		//Exit if the paths not provided.
		if(args.length!=2){
			System.out.println("Invalid Parameters");
			return -1;
		}
        //Creating a Job
        Configuration conf = getConf();
        while(terminationValue>0){
    	String ip,op;
        Job job = new Job(conf, "Single Source Shortest Path");
      
        //Driver Class
        job.setJarByClass(SSSPHadoop.class);
        
        //Setting Mapper, Reducer and Partitioner Classes
        job.setMapperClass(SSSPMapper.class);
        job.setReducerClass(SSSPReducer.class);
        job.setPartitionerClass(SSSPPartitioner.class);
        
        //Setting Mapper Key and Value output type
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Text.class);

        //Setting final Key and Value Type
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);
        
        //Set number of Reducers
        job.setNumReduceTasks(5);

        //Creating file-paths
    	if(iterationCount==0){
			ip=args[0];	
    	}
    	else{	
			ip=args[1]+iterationCount;
    	}
    	op = args[1]+(iterationCount+1);	
        
        //Setting the paths
        FileInputFormat.setInputPaths(job, new Path(ip));
        FileOutputFormat.setOutputPath(job, new Path(op));
        
        //Running the job.
        job.waitForCompletion(true);
        
        //Getting the value of counter.
    	terminationValue= job.getCounters().findCounter(MoreIterations.NUMBEROFITERATIONS).getValue();
        iterationCount++;
        }
        return 0;
    }

    public static void main(String[] args) throws Exception {
    	
        int res = ToolRunner.run(new Configuration(), new SSSPHadoop(), args);
        System.exit(res);
    }
	
}
