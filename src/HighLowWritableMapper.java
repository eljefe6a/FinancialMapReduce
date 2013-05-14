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

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Hadoop MapReduce example showing a custom Writable
 *
 */
public class HighLowWritableMapper extends Mapper<LongWritable, Text, Text, StockWritable> {
	/**
	 * Expected input:<br>
	 * 
	 * <pre>
	 * exchange,stock_symbol,date,stock_price_open,stock_price_high,stock_price_low,stock_price_close,stock_volume,stock_price_adj_close
	 * NASDAQ,XING,2010-02-08,1.73,1.76,1.71,1.73,147400,1.73
	 * </pre>
	 */
	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String inputLine = value.toString();
		
		if (inputLine.startsWith("exchange,")) {
			// Line is the header, ignore it
			return;
		}
		
		String[] columns = inputLine.split(",");
		
		if (columns.length != 9) {
			// Line isn't the correct number of columns or formatted properly
			return;
		}

		BigDecimal close = new BigDecimal(columns[6]);
		StockWritable stockWritable = new StockWritable(columns[2], close);
		
		context.write(new Text(columns[1]), stockWritable);
	}
}