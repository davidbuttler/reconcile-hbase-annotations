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

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.ToolRunner;

import com.google.common.collect.Maps;
import com.google.gson.Gson;

import gov.llnl.text.util.MapUtil;

import reconcile.data.ByteOffsetMatch;
import reconcile.hbase.table.DocSchema;

import reconcile.data.Annotation;
import reconcile.data.AnnotationSet;
import reconcile.general.Constants;
import reconcile.hbase.mapreduce.ChainableAnnotationJob;
import reconcile.hbase.mapreduce.JobConfig;
import reconcile.hbase.mapreduce.annotation.AnnotationUtils;
import reconcile.hbase.mapreduce.annotation.ReportProgressThread;
import reconcile.hbase.parser.StanfordCoref;

/**
 * Map/Reduce task for annotating Named Entities
 *
 * @author David Buttler
 *
 */
public class StanfordCorefAnnotation
    extends ChainableAnnotationJob {

private static final int MAX_RAW_TEXT_LENGTH = 1024 * 1024 * 10;

public static void main(String[] args)
{
  try {
    ToolRunner.run(new Configuration(), new StanfordCorefAnnotation(), args);
  }
  catch (Exception e) {
    e.printStackTrace();
  }

}

@Override
public void init(JobConfig jobConfig, Job job, Scan scan)
{
  job.getConfiguration().set("mapred.child.java.opts", "-Xmx6g");
  scan.addColumn(DocSchema.textCF.getBytes(), DocSchema.textRaw.getBytes());
  scan.addFamily(DocSchema.annotationsCF.getBytes());
}

@Override
public Class<? extends AnnotateMapper> getMapperClass()
{
  return CorefMapper.class;
}

public static class CorefMapper
    extends AnnotateMapper {

private static final int MAX_SENTENCE_LENGTH = 100;

private StanfordCoref parser;

private int nFiles;

private Gson gson;

// private JobConf mJobConf;

// private static final Pattern pSpace = Pattern.compile("\\s");
/**
*
*/
public CorefMapper() {
  gson = new Gson();

}

@Override
public void setup(Context context)
{
  try {
    super.setup(context);
    parser = new StanfordCoref();
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
public void map(ImmutableBytesWritable mapKey, Result row, Context context)
    throws IOException, InterruptedException
{
  // keep alive for long parses
  ReportProgressThread progress = null;
  try {

    nFiles++;
    context.getCounter("stanford coref", "row counter").increment(1);

    if (alreadyParsed(row)) return;

    // Get the raw text
    String rawText = DocSchema.getRawText(row);
    // nothing to do
    if (rawText == null || rawText.trim().equals("")) {
      context.getCounter("stanford coref", "skip -- raw text empty").increment(1);
      return;
    }

    if (rawText.length() > MAX_RAW_TEXT_LENGTH) {
      context.getCounter("stanford coref", "skip -- raw text too long (" + MAX_RAW_TEXT_LENGTH + ")").increment(1);
      return;
    }

    AnnotationSet parse = DocSchema.getAnnotationSet(row, StanfordCoref.STANFORD_COREF);
    if (parse != null && parse.size() > 0) {
      context.getCounter("stanford coref", "skip -- already parsed ").increment(1);
      return;
    }

    // check sentence length
    AnnotationSet sentence = DocSchema.getAnnotationSet(row, Constants.SENT);
    AnnotationSet tokens = DocSchema.getAnnotationSet(row, Constants.TOKEN);
    if (tokens != null && sentence != null) {
      for (Annotation s : sentence) {
        AnnotationSet sentTokens = tokens.getOverlapping(s);
        if (sentTokens.size() > MAX_SENTENCE_LENGTH) {
          context.getCounter("stanford coref",
              "skip -- sentence too long (" + sentTokens.size() + " > " + MAX_SENTENCE_LENGTH + ")").increment(1);
          return;
        }
      }
    }

    // start keep alive for long parses
    progress = ReportProgressThread.start(context, 10000);

    boolean add = false;
    Put put = new Put(mapKey.get());

    // now parse out the named entities
    Map<String, AnnotationSet> map = parser.parse(rawText);

    for (String key : map.keySet()) {
      AnnotationSet as = map.get(key);
      if (as.size() > 0) {
        context.getCounter("stanford coref", "found annotation: " + key).increment(1);
        DocSchema.add(put, DocSchema.annotationsCF, key, AnnotationUtils.getAnnotationStr(as));
        add = true;
      }
    }

    if (add) {
      try {
        docTable.put(put);
        context.getCounter("stanford coref", "update row").increment(1);
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
      context.getCounter("stanford coref", "nothing to add").increment(1);
    }

  }
  catch (Exception e) {
    context.getCounter("stanford ner error", e.getMessage()).increment(1);
    LOG.error("row for key(" + Bytes.toString(mapKey.get()) + ") failed. reason: " + e.toString());
  }
  finally {
    if (progress != null) {
      progress.interrupt();
    }
  }
}

private boolean alreadyParsed(Result row)
{
  AnnotationSet as = DocSchema.getAnnotationSet(row, Constants.COREF);
  if (as != null && as.size() > 0) return true;
  return false;
}

@edu.umd.cs.findbugs.annotations.SuppressWarnings(value = "WMI_WRONG_MAP_ITERATOR")
public Map<String, String> getAnnotationStr(String text, String prefix, AnnotationSet annSet)
{
  if (annSet == null) return null;
  Map<String, String> map = Maps.newHashMap();
  Map<String, List<ByteOffsetMatch>> mapList = Maps.newHashMap();
  for (Annotation a : annSet) {
    String val = Annotation.getAnnotText(a, text);
    ByteOffsetMatch b = new ByteOffsetMatch(val, a.getStartOffset(), a.getEndOffset());
    MapUtil.addToMapList(mapList, a.getType(), b);
  }
  for (String key : mapList.keySet()) {
    List<ByteOffsetMatch> list = mapList.get(key);
    String newKey = prefix + key.toLowerCase().trim();
    map.put(newKey, gson.toJson(list));
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
