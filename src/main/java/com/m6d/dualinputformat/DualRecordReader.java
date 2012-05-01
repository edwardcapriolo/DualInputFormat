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
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;

public class DualRecordReader implements RecordReader<Text,Text>{

  boolean hasNext=true;

  public DualRecordReader(JobConf jc, InputSplit s) {

  }

  public DualRecordReader(){
    
  }

  @Override
  public long getPos() throws IOException {
    return 0;
  }

  @Override
  public void close() throws IOException {
  }

  @Override
  public float getProgress() throws IOException {
    if (hasNext)
      return 0.0f;
    else
      return 1.0f;
  }

  @Override
  public Text createKey() {
    return new Text("");
  }

  @Override
  public Text createValue() {
    return new Text("");
  }

  @Override
  public boolean next(Text k, Text v) throws IOException {
    if (hasNext){
      hasNext=false;
      return true;
    } else {
      return hasNext;
    }
  }

}