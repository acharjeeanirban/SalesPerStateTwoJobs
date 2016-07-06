import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class CustomerIdGroupComparator extends WritableComparator {
	protected CustomerIdGroupComparator() {
		super(CompositeKeyForCustomerId.class, true);
	}

	@Override
	public int compare(WritableComparable w1, WritableComparable w2) {

		CompositeKeyForCustomerId key1 = (CompositeKeyForCustomerId) w1;
		CompositeKeyForCustomerId key2 = (CompositeKeyForCustomerId) w2;
		return key1.getCustomerId().compareTo(key2.getCustomerId());
	}
}
