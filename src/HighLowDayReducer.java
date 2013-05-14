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
