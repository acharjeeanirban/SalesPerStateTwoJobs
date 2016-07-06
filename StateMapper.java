import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class StateMapper extends Mapper<Text, Text, Text, Text> {
	
  public static final Log log = LogFactory.getLog(StateMapper.class);

  @Override
  public void map(Text key, Text value, Context context)
      throws IOException, InterruptedException {
	  String arr[] = value.toString().split(",");
	  log.info("inside 2nd mapper");
	  log.info("inside 2nd mapper, the state " + arr[0]);
	  log.info("inside 2nd mapper, the sales price " + arr[1]);
	
	  context.write(new Text(arr[0]), new Text(arr[1]));
	  
  }
}
