/**
 * Copyright 2013 Jesse Anderson
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
