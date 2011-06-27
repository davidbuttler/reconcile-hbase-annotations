/*
 * Copyright (c) 2008, Lawrence Livermore National Security, LLC. Produced at the Lawrence Livermore National
 * Laboratory. Written by David Buttler, buttler1@llnl.gov All rights reserved.
 *
 * This program is free software; you can redistribute it and/or modify it under the terms of the GNU General Public
 * License (as published by the Free Software Foundation) version 2, dated June 1991. This program is distributed in the
 * hope that it will be useful, but WITHOUT ANY WARRANTY; without even the IMPLIED WARRANTY OF MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE. See the terms and conditions of the GNU General Public License for more details.
 * You should have received a copy of the GNU General Public License along with this program; if not, write to the Free
 * Software Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA 02111-1307 USA For full text see license.txt
 */
package reconcile.hbase.mapreduce.annotation;

import static reconcile.hbase.table.DocSchema.annotationsCF;
import static reconcile.hbase.table.DocSchema.textCF;
import static reconcile.hbase.table.DocSchema.textRaw;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Arrays;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.ToolRunner;

import reconcile.data.Annotation;
import reconcile.data.AnnotationSet;
import reconcile.hbase.mapreduce.ChainableAnnotationJob;
import reconcile.hbase.mapreduce.JobConfig;
import reconcile.hbase.table.DocSchema;

/**
 * Check to see if all annotations are within doc length
 *
 * @author David Buttler
 *
 */
public class AnnotationChecker
    extends ChainableAnnotationJob {

static final Log LOG = LogFactory.getLog(AnnotationChecker.class);

/**
 *
 */
public static void main(String[] args)
{
  try {
    ToolRunner.run(new Configuration(), new AnnotationChecker(), args);
  }
  catch (Exception e) {
    e.printStackTrace();
  }

}

@Override
public void init(JobConfig jobConfig, Job job, Scan scan)
{
  scan.addColumn(textCF.getBytes(), textRaw.getBytes());
  scan.addFamily(annotationsCF.getBytes());
}

@Override
public Class<? extends AnnotateMapper> getMapperClass()
{
  return CheckMapper.class;
}

public static class CheckMapper
    extends AnnotateMapper {

/**
*
*/
public CheckMapper() {

}

@Override
public void setup(Context context)
{
  try {
    super.setup(context);
  }
  catch (FileNotFoundException e) {
    e.printStackTrace();
  }
  catch (IOException e) {
    e.printStackTrace();
  }
  catch (InterruptedException e) {
    e.printStackTrace();
  }

}

@Override
public void map(ImmutableBytesWritable mapKey, Result row, Context context)
    throws IOException, InterruptedException
{
  context.getCounter("Token Annotation", "row counter").increment(1);

  // Get the raw text
  String rawText = DocSchema.getRawText(row);
  if (rawText == null || rawText.length() == 0) {
    context.getCounter("Token Annotation", "skip -- no raw text").increment(1);
    return;
  }
  int length = rawText.length();
  byte[] annotationFam = DocSchema.annotationsCF.getBytes();
  for (KeyValue kv : row.list()) {
    // is an annotation
    if (Arrays.equals(kv.getFamily(), annotationFam)) {
      AnnotationSet as = DocSchema.getAnnotationSet(kv);
      for (Annotation a : as) {
        if (a.getStartOffset() < length && a.getEndOffset() < length) {
          context.getCounter(contextHeader(), "good").increment(1);
        }
        if (a.getStartOffset() > length) {
          context.getCounter(contextHeader(), "bad start").increment(1);
        }
        if (a.getEndOffset() > length) {
          context.getCounter(contextHeader(), "bad end").increment(1);
        }
        if (a.getStartOffset() >= a.getEndOffset()) {
          context.getCounter(contextHeader(), "invalid annotation").increment(1);
        }
      }
    }
  }

}

}
}
