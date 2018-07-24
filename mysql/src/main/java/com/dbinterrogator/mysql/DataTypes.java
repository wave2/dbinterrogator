/*
 * Copyright (c) 2018 Wave2 Limited
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

package com.dbinterrogator.mysql;

public class DataTypes {

    public long getMaxValue(String dataType, boolean signed){
        long maxValue = 0;
        if (signed) {
            switch (dataType) {
                case "tinyint":
                    maxValue = 127;
                    break;
                case "smallint":
                    maxValue = 32767;
                    break;
                case "mediumint":
                    maxValue = 8388607;
                    break;
                case "int":
                    maxValue = 2147483647;
                    break;
                case "bigint":
                    maxValue = Long.parseUnsignedLong("9223372036854775807");
                    break;
            }
        } else {
            switch (dataType) {
                case "tinyint":
                    maxValue = 255;
                    break;
                case "smallint":
                    maxValue = 65535;
                    break;
                case "mediumint":
                    maxValue = 16777215;
                    break;
                case "int":
                    maxValue = Long.parseUnsignedLong("4294967295");
                    break;
                case "bigint":
                    maxValue = Long.parseUnsignedLong("18446744073709551615");
                    break;
            }
        }
        return maxValue;
    }

}
