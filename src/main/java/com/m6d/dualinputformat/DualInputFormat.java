/*
Copyright 2011 m6d.com

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/
package com.m6d.dualinputformat;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;

public class DualInputFormat implements InputFormat{

  @Override
  public InputSplit[] getSplits(JobConf jc, int i) throws IOException {
    InputSplit [] splits = new DualInputSplit[1];
    splits[0]= new DualInputSplit();
    return splits;
  }

  @Override
  public RecordReader<Text,Text> getRecordReader(InputSplit split, JobConf jc,
          Reporter rprtr) throws IOException {
    return new DualRecordReader(jc, split);
  }

}
