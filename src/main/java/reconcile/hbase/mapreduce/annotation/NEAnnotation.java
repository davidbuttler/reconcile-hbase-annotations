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
 *
 */
package reconcile.hbase.mapreduce.annotation;

import static reconcile.hbase.mapreduce.annotation.AnnotationUtils.getAnnotationStr;
import static reconcile.hbase.table.DocSchema.annotationsCF;
import static reconcile.hbase.table.DocSchema.textCF;
import static reconcile.hbase.table.DocSchema.textRaw;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.ToolRunner;

import gov.llnl.text.util.MapUtil;

import reconcile.data.Annotation;
import reconcile.data.AnnotationSet;
import reconcile.featureExtractor.NamedEntityStanford;
import reconcile.featureExtractor.ParagraphSplitter;
import reconcile.featureExtractor.SentenceSplitterOpenNLP;
import reconcile.general.Constants;
import reconcile.hbase.mapreduce.ChainableAnnotationJob;
import reconcile.hbase.mapreduce.JobConfig;
import reconcile.hbase.table.DocSchema;

/**
 * Map/Reduce task for annotating Named Entities
 *
 * @author David Buttler
 *
 */
public class NEAnnotation extends ChainableAnnotationJob {


/**
 *
 * @param args
 *          :
 *          <ol>
 *          <li>optional source argument to limit the rows to tag
 *          <li>-keyListFile=<hdfs file containing keys> optional argument to specify processing of only select rows
 *          </ol>
 */
public static void main(String[] args)
{
  try {
    ToolRunner.run(new Configuration(), new NEAnnotation(), args);
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
	return NEMapper.class;
}


public static class NEMapper extends AnnotateMapper {

private static final int MAX_ROW_LENGTH = 1024 * 1024 * 10;

private static final int MAX_RAW_TEXT_LENGTH = 1024 * 1024; // * 10;

private NamedEntityStanford parser;

private SentenceSplitterOpenNLP sentenceSplitter;

private ParagraphSplitter paragraphSplitter;

private int nFiles;

// private JobConf mJobConf;

// private static final Pattern pSpace = Pattern.compile("\\s");
/**
*
*/
public NEMapper() {

  try {
    paragraphSplitter = new ParagraphSplitter();
    sentenceSplitter = new SentenceSplitterOpenNLP();
    parser = new NamedEntityStanford();
  }
  catch (Exception e) {
    e.printStackTrace();
    ChainableAnnotationJob.LOG.info(e.getMessage());
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
@edu.umd.cs.findbugs.annotations.SuppressWarnings(value = "WMI_WRONG_MAP_ITERATOR")
public void map(ImmutableBytesWritable mapKey, Result mapValue, Context context)
    throws IOException, InterruptedException
{
  // keep alive for long parses
  ReportProgressThread progress = null;
  try {

    if (isRowSizeTooLarge(mapValue, context, MAX_ROW_LENGTH)) return;
    nFiles++;
    context.getCounter("named entities", "row counter").increment(1);

    AnnotationSet namedEntities = DocSchema.getAnnotationSet(mapValue, Constants.NE);
    // our job is done here
    if (namedEntities != null && namedEntities.size() != 0) {
      context.getCounter("named entities", "skip -- already annotated").increment(1);
      return;
    }

    // Get the raw text
    String rawText = DocSchema.getRawText(mapValue);
    if (isRawTextInvalidSize(rawText, context, MAX_RAW_TEXT_LENGTH)) return;

    // start keep alive for long parses
    progress = ReportProgressThread.start(context, 10000);

    boolean add = false;
    Put put = new Put(mapKey.get());

    AnnotationSet parSet = DocSchema.getAnnotationSet(mapValue, Constants.PAR);
    if (parSet == null || parSet.size() == 0) {
      parSet = paragraphSplitter.parse(rawText, Constants.PAR);
      addField(mapValue, put, DocSchema.annotationsCF, Constants.PAR, getAnnotationStr(parSet).getBytes());
      context.getCounter("named entities", "add paragraphs").increment(1);
      add = true;
    }

    // get the sentences from precomputed annotation set on disk
    AnnotationSet sentences = DocSchema.getAnnotationSet(mapValue, Constants.SENT);
    if (sentences == null || sentences.size() == 0) {
      sentences = sentenceSplitter.parse(rawText, parSet, Constants.SENT);
      addField(mapValue, put, DocSchema.annotationsCF, Constants.SENT, getAnnotationStr(sentences).getBytes());
      context.getCounter("named entities", "add sentences").increment(1);
      add = true;
    }

    // now parse out the named entities
    if (namedEntities == null || namedEntities.size() == 0) {
      namedEntities = new AnnotationSet(Constants.NE);

      for (Annotation sent : sentences) {
        String sentText = Annotation.getAnnotText(sent, rawText);
        int sentStart = sent.getStartOffset();
        parser.parseSentence(sentText, sentStart, namedEntities);
      }

      Map<String, Set<String>> map = classify(namedEntities, rawText);

      // write output
      // write named entity sets to meta data
      for (String key : map.keySet()) {
        Set<String> val = map.get(key);
        addField(mapValue, put, DocSchema.metaCF, key, AnnotationUtils.toString(val).getBytes());
        add = true;
      }
      String neStr = getAnnotationStr(namedEntities);
      if (neStr != null && neStr.trim().length() > 0) {
        addField(mapValue, put, DocSchema.annotationsCF, Constants.NE, neStr.getBytes());
        context.getCounter("named entities", "add ne").increment(1);
        add = true;
      }
    }

    if (add) {
      try {
        docTable.put(put);
        context.getCounter("named entities", "update row").increment(1);
      }
      catch (IOException e) {
        context.getCounter("stanford ner error", "io exception in put").increment(1);
        e.printStackTrace();
      }
      catch (IllegalArgumentException e) {
        context.getCounter("stanford ner error", "illegal arg exception in put").increment(1);
        e.printStackTrace();
      }
    }
    else {
      context.getCounter("named entities", "nothing to add").increment(1);
    }

  }
  catch (Exception e) {
    context.getCounter("stanford ner error", e.getMessage()).increment(1);
  }
  finally {
    if (progress != null) {
      progress.interrupt();
    }
  }
}

/**
 * put the named entities in a map keyed by the NE type
 *
 * @param doc
 */
private Map<String, Set<String>> classify(AnnotationSet ne, String text)
{
  // Track matches
  HashMap<String, Set<String>> map = new HashMap<String, Set<String>>();

  for (Annotation a : ne) {
    String type = a.getType();
    String neText = Annotation.getAnnotText(a, text);
    MapUtil.addToMapSet(map, type, neText);
  }
  return map;
}

@Override
public void cleanup(Context context1)
    throws IOException, InterruptedException
{
  super.cleanup(context1);
  System.out.println("processed " + nFiles + " files");
}

}

}
