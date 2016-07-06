import java.io.IOException;


import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class SalesPerCustomerReducer extends Reducer<CompositeKeyForCustomerId, Text, Text, Text> {
	
  public static final Log log = LogFactory.getLog(SalesPerCustomerReducer.class);
  StringBuilder strBuilder = new StringBuilder("");
  Text reduceOutput = new Text("");

  @Override
  public void reduce(CompositeKeyForCustomerId key, Iterable<Text> values, Context context)
      throws IOException, InterruptedException {

	  String commaSeparator = ",";
	  log.info("inside first reducer");
	  int sum = 0;
	  String cusId = null;
	  for (Text text : values) {
		  if (key.getIndex() == 1) {
			  cusId = key.getCustomerId();
			  log.info("bazinga inside reduce reducer, cusid " + cusId);
			  strBuilder.append(text.toString()).append(commaSeparator);
		  } else {
			  log.info("bazinga inside reduce reducer, sales price " + Integer.parseInt(text.toString()));
			  sum += Integer.parseInt(text.toString());
		  }
	  }
	  strBuilder.append(sum);
	  reduceOutput.set(strBuilder.toString());
	  context.write(new Text(cusId), reduceOutput);
	  strBuilder.setLength(0);
	  reduceOutput.set("");
  }
}