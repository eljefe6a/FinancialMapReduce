import java.io.IOException;
import java.math.BigDecimal;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Hadoop MapReduce example showing a custom Writable
 *
 */
public class HighLowWritableReducer extends Reducer<Text, StockWritable, Text, Text> {

	@Override
	public void reduce(Text key, Iterable<StockWritable> values, Context context) throws IOException,
			InterruptedException {
		StockWritable high = new StockWritable("", new BigDecimal(0));
		StockWritable low = new StockWritable("", new BigDecimal(Double.MAX_VALUE));

		// Go through all values to find the high and low
		for (StockWritable value : values) {
			if (value.price.compareTo(high.price) == 1) {
				// Must clone because MapReduce reuses the same StockWritable objects
				high = (StockWritable) value.clone();
			}

			if (value.price.compareTo(low.price) == -1) {
				// Must clone because MapReduce reuses the same StockWritable objects
				low = (StockWritable) value.clone();
			}
		}

		Text value = new Text("High:" + high.price.toPlainString() + " on " + high.date + " Low:"
				+ low.price.toPlainString() + " on " + low.date);

		context.write(key, value);
	}
}
