import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class CustomerIdMapper extends Mapper<LongWritable, Text, CompositeKeyForCustomerId, Text> {
	
  public static final Log log = LogFactory.getLog(CustomerIdMapper.class);

  @Override
  public void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {
	  
	  CompositeKeyForCustomerId compositeKeyForCustomerId = new CompositeKeyForCustomerId();
	  
	  log.info("inside 1st mapper");	  
	  if (value.toString().length() > 0) {
		  String arrEntityAttributes[] = value.toString().split(",");
		  if (arrEntityAttributes.length > 3) {
			  log.info("inside mapper more than 3");
			  String customerId = arrEntityAttributes[0];
			  String state = arrEntityAttributes[4];
			  compositeKeyForCustomerId.setCustomerId(customerId);
			  compositeKeyForCustomerId.setIndex(1);
			  context.write(compositeKeyForCustomerId, new Text(state));
		  } else {
			  log.info("inside mapper less than 3");
			  String customerId = arrEntityAttributes[1];
			  String salesPrice = arrEntityAttributes[2];
			  compositeKeyForCustomerId.setCustomerId(customerId);
			  compositeKeyForCustomerId.setIndex(2);
			  context.write(compositeKeyForCustomerId, new Text(salesPrice));
		  }

	  }

  }
}
