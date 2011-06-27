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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.ToolRunner;

import reconcile.data.Annotation;
import reconcile.data.AnnotationSet;
import reconcile.data.Document;
import reconcile.featureExtractor.BaseNPAnnotator;
import reconcile.general.Constants;
import reconcile.hbase.ReconcileDocument;
import reconcile.hbase.mapreduce.ChainableAnnotationJob;
import reconcile.hbase.mapreduce.JobConfig;
import reconcile.hbase.table.DocSchema;

/**
 * Run the base annotators on a document
 *
 * @author David Buttler
 *
 */
public class BaseNPAnnotation extends ChainableAnnotationJob {

/**
 *
 * @param args
 *          :
 *          <ol>
 *          <li>optional start row
 *          </ol>
 */
public static void main(String[] args)
{
  try {
    ToolRunner.run(new Configuration(), new BaseNPAnnotation(), args);
  }
  catch (Exception e) {
    e.printStackTrace();
  }

}

@Override
public void init(JobConfig jobConfig, Job job, Scan scan)
{
    String startRow = null;
    
    if (jobConfig.getArgs().length > 0) {
    	startRow = jobConfig.getArgs()[0];
    }

    job.getConfiguration().set("mapred.tasktracker.map.tasks.maximum", "5");

    scan.addColumn(textCF.getBytes(), textRaw.getBytes());
    scan.addFamily(annotationsCF.getBytes());

    if (startRow != null) {
      scan.setStartRow(startRow.getBytes());
    }
}

@Override
public Class<? extends AnnotateMapper> getMapperClass()
{
  return BaseNPMapper.class;
}


public static class BaseNPMapper
    extends AnnotateMapper {


// private JobConf mJobConf;

// private static final Pattern pSpace = Pattern.compile("\\s");
/**
*
*/
public BaseNPMapper() {

  try {
  }
  catch (Exception e) {
    e.printStackTrace();
    LOG.info(e.getMessage());
    throw new RuntimeException(e);
  }

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
  context.getCounter("Base NP Annotation", "row counter").increment(1);

  // keep alive for long parses
  ReportProgressThread progress = null;
  try {
    // start keep alive for long parses
    progress = ReportProgressThread.start(context, 1000);

    // Get the raw text
    String rawText = DocSchema.getRawText(row);
    if (rawText == null || rawText.length() == 0) {
      context.getCounter("Base Reconcile Annotation", "skip -- no raw text").increment(1);
      return;
    }

    Document doc = new ReconcileDocument(row);
    AnnotationSet pos = doc.getAnnotationSet(Constants.POS);
    if (pos == null || pos.size() == 0) {
      context.getCounter("Base NP Annotation", "skip -- no POS").increment(1);
    }
    AnnotationSet parse = doc.getAnnotationSet(Constants.PARSE);
    if (parse == null || parse.size() == 0) {
      context.getCounter("Base NP Annotation", "skip -- not parsed").increment(1);
    }

    AnnotationSet ne = doc.getAnnotationSet(Constants.NE);
    if (ne == null) {
      context.getCounter("Base NP Annotation", "no NEs").increment(1);
      ne = new AnnotationSet(Constants.NE);
      doc.addAnnotationSet(ne, false);
    }


    // create put
    Put put = new Put(mapKey.get());
    boolean add = false;

    AnnotationSet npSet = annotate(doc);
    if (npSet != null && npSet.size() > 0) {
      addAnnotation(row, put, npSet, npSet.getName());
      add = true;
    }

    // write output
    // add annotation text
    if (add) {
      try {
        docTable.put(put);
        docTable.flushCommits();
        context.getCounter("Base NP Annotation", "put").increment(1);
      }
      catch (IOException e) {
        context.getCounter("Base NP Annotation error", "io exception in put").increment(1);
        e.printStackTrace();
        LOG.error("row for key("+Bytes.toString(mapKey.get())+") failed. reason: "+e.toString());
      }
      catch (IllegalArgumentException e) {
        context.getCounter("Base NP Annotation error", "illegal arg exception in put").increment(1);
        e.printStackTrace();
        LOG.error("row for key("+Bytes.toString(mapKey.get())+") failed. reason: "+e.toString());
      }
    }
    else {
      context.getCounter("Base Reconcile Annotation", "skip -- no columns to add").increment(1);
    }
  }
  finally {
    if (progress != null) {
      progress.interrupt();
    }
  }

}

// This method extracts the base NPs that are used in coreference resolution
// Uses the MUC definition of NP
public AnnotationSet annotate(Document doc)
{
  doc.loadAnnotationSetsByName(new String[] { Constants.PARSE, Constants.POS, Constants.NE });
  AnnotationSet bnp = BaseNPAnnotator.run(Constants.NP, doc);

  Annotation problem;
  if ((problem = bnp.checkForCrossingAnnotations()) != null)
    throw new RuntimeException("Crossing annotation: " + problem);
  return bnp;

}


}
}
