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

import static reconcile.hbase.mapreduce.annotation.AnnotationUtils.getAnnotationStr;
import static reconcile.hbase.table.DocSchema.annotationsCF;
import static reconcile.hbase.table.DocSchema.textCF;
import static reconcile.hbase.table.DocSchema.textRaw;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.ToolRunner;

import com.google.common.collect.Lists;

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
public class BadAnnotationStripper
    extends ChainableAnnotationJob {

static final Log LOG = LogFactory.getLog(BadAnnotationStripper.class);

/**
 *
 */
public static void main(String[] args)
{
  try {
    ToolRunner.run(new Configuration(), new BadAnnotationStripper(), args);
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
  context.getCounter("Bad Annote", "row counter").increment(1);

  // Get the raw text
  String rawText = DocSchema.getRawText(row);
  if (rawText == null || rawText.length() == 0) {
    context.getCounter("Bad Annote", "skip -- no raw text").increment(1);
    return;
  }
  int length = rawText.length();
  byte[] annotationFam = DocSchema.annotationsCF.getBytes();
  boolean changed = false;
  Put p = new Put(row.getRow());

  for (KeyValue kv : row.list()) {
    // is an annotation
    if (Arrays.equals(kv.getFamily(), annotationFam)) {
      AnnotationSet as = DocSchema.getAnnotationSet(kv);
      boolean localChanged = false;
      List<Annotation> list = Lists.newArrayList();
      for (Annotation a : as) {
        if (a.getStartOffset() < length && a.getEndOffset() < length) {
          context.getCounter(contextHeader(), "good").increment(1);
        }
        if (a.getStartOffset() > length) {
          localChanged = true;
          context.getCounter(contextHeader(), "bad start").increment(1);
          list.add(a);
        }
        if (a.getEndOffset() > length) {
          localChanged = true;
          context.getCounter(contextHeader(), "bad end").increment(1);
          list.add(a);
        }
        if (a.getStartOffset() >= a.getEndOffset()) {
          localChanged = true;
          context.getCounter(contextHeader(), "invalid annotation").increment(1);
          list.add(a);
        }
      }
      if (list.size() > 0) {
        for (Annotation a : list) {
          boolean removed = as.remove(a);
          if (!removed) {
            context.getCounter(contextHeader(), "didn't remove bad annote").increment(1);
          }
        }

      }
      if (localChanged) {
        changed = true;
        byte[] data = getAnnotationStr(as).getBytes();
        p.add(kv.getFamily(), kv.getQualifier(), data);
        context.getCounter(contextHeader(), "add put").increment(1);
      }
    }
  }
  if (changed) {

    try {
      docTable.put(p);
      context.getCounter(contextHeader(), "put").increment(1);
    }
    catch (IOException e) {
      context.getCounter(contextHeader() + " error", "io exception in put").increment(1);
      e.printStackTrace();
    }
    catch (IllegalArgumentException e) {
      context.getCounter(contextHeader() + " error", "illegal arg exception in put").increment(1);
      e.printStackTrace();
    }

  }

}

}
}
