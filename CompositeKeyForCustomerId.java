import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;


public class CompositeKeyForCustomerId implements Writable,
		WritableComparable<CompositeKeyForCustomerId> {
	
		
		private String customerId;
		private int index;

		public CompositeKeyForCustomerId() {
		}

		public CompositeKeyForCustomerId(String customerId, int index) {
			this.customerId = customerId;
			this.index = index;
		}

		@Override
		public String toString() {

			return (new StringBuilder().append(customerId).append("\t")
					.append(index)).toString();
		}

		public void readFields(DataInput dataInput) throws IOException {
			customerId = WritableUtils.readString(dataInput);
			index = WritableUtils.readVInt(dataInput);
		}

		public void write(DataOutput dataOutput) throws IOException {
			WritableUtils.writeString(dataOutput, customerId);
			WritableUtils.writeVInt(dataOutput, index);
		}

		public int compareTo(CompositeKeyForCustomerId compositeKeyForCustomerId) {
			
			int result = customerId.compareTo(compositeKeyForCustomerId.customerId);
			if (0 == result) {
				result = Double.compare(index, compositeKeyForCustomerId.index);
			}
			return result;
		}

		public String getCustomerId() {
			return customerId;
		}

		public void setCustomerId(String customerId) {
			this.customerId = customerId;
		}

		public int getIndex() {
			return index;
		}

		public void setIndex(int index) {
			this.index = index;
		}

}
