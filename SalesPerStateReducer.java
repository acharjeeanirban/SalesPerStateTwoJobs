import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class SalesPerStateReducer extends Reducer<Text, Text, Text, DoubleWritable> {
	  
  public static final Log log = LogFactory.getLog(SalesPerStateReducer.class);

  @Override
  public void reduce(Text key, Iterable<Text> values, Context context)
      throws IOException, InterruptedException {

	  log.info("inside second reducer");
	  int sum = 0;
	  for (Text text : values) {
		  sum += Integer.parseInt(text.toString());
	  }
	  
	  context.write(key, new DoubleWritable(sum));

  }
}