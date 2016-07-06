import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;


// This is not used.
public class SortingComparator extends WritableComparator {

	protected SortingComparator() {
		super(CompositeKeyForCustomerId.class, true);
	}

	@Override
	public int compare(WritableComparable w1, WritableComparable w2) {

		CompositeKeyForCustomerId key1 = (CompositeKeyForCustomerId) w1;
		CompositeKeyForCustomerId key2 = (CompositeKeyForCustomerId) w2;

		int result = key1.getCustomerId().compareTo(key2.getCustomerId());
		if (result == 0) {
			return Double.compare(key1.getIndex(), key2.getIndex());
		}
		return result;
	}
}