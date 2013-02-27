import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Hadoop MapReduce example showing high and low for a day across all stock symbols
 * 
 */
public class HighLowDayReducer extends Reducer<Text, DoubleWritable, Text, Text> {

	@Override
	public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException,
			InterruptedException {
		double high = 0;
		double low = Double.MAX_VALUE;

		// Go through all values to find the high and low
		for (DoubleWritable value : values) {
			if (value.get() > high) {
				high = value.get();
			}
			
			if (value.get() < low) {
				low = value.get();
			}
		}

		Text value = new Text("High:" + high + " Low:" + low);
		
		context.write(key, value);
	}
}
