
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class CustomerIdPartitioner extends Partitioner<CompositeKeyForCustomerId, Text> {

	@Override
	public int getPartition(CompositeKeyForCustomerId key, Text value,
			int numReduceTasks) {
		return (key.getCustomerId().hashCode() % numReduceTasks);
	}
}
