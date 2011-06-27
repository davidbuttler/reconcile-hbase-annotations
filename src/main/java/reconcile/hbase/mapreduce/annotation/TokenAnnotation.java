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
import static reconcile.hbase.table.DocSchema.add;
import static reconcile.hbase.table.DocSchema.annotationsCF;
import static reconcile.hbase.table.DocSchema.textCF;
import static reconcile.hbase.table.DocSchema.textRaw;

import java.io.DataInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.zip.GZIPInputStream;

import opennlp.maxent.MaxentModel;
import opennlp.maxent.io.BinaryGISModelReader;
import opennlp.tools.tokenize.TokenizerME;
import opennlp.tools.util.Span;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.ToolRunner;

import reconcile.data.Annotation;
import reconcile.data.AnnotationSet;
import reconcile.general.Constants;
import reconcile.hbase.mapreduce.ChainableAnnotationJob;
import reconcile.hbase.mapreduce.JobConfig;
import reconcile.hbase.table.DocSchema;

/**
 * Run the base annotators on a document
 *
 * @author David Buttler
 *
 */
public class TokenAnnotation
    extends ChainableAnnotationJob {

static final Log LOG = LogFactory.getLog(TokenAnnotation.class);

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
    ToolRunner.run(new Configuration(), new TokenAnnotation(), args);
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
  return TokenMapper.class;
}

public static class TokenMapper
    extends AnnotateMapper {

private static final String OPEN_NLP_TOKENIZER_MODEL = "OpenNLP/models/EnglishTok.bin.gz";

private static final int MAX_RAW_TEXT_LENGTH = 1024 * 1024 * 10;

private TokenizerME tknzr;

/**
*
*/
public TokenMapper() {

  try {

    // tokenizer
    InputStream resStream = this.getClass().getClassLoader().getResourceAsStream(OPEN_NLP_TOKENIZER_MODEL);
    DataInputStream dis = new DataInputStream(new GZIPInputStream(resStream));
    tknzr = new TokenizerME(getModel(dis));

  }
  catch (Exception e) {
    e.printStackTrace();
    TokenAnnotation.LOG.info(e.getMessage());
    throw new RuntimeException(e);
  }

}

private static MaxentModel getModel(DataInputStream in)
{
  try {
    return new BinaryGISModelReader(in).getModel();
  }
  catch (IOException e) {
    e.printStackTrace();
    return null;
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
  context.getCounter("Token Annotation", "row counter").increment(1);

  // keep alive for long parses
  ReportProgressThread progress = null;
  try {
    // start keep alive for long parses
    progress = ReportProgressThread.start(context, 1000);

    // Get the raw text
    String rawText = DocSchema.getRawText(row);
    if (rawText == null || rawText.length() == 0) {
      context.getCounter("Token Annotation", "skip -- no raw text").increment(1);
      return;
    }
    if (rawText.length() > MAX_RAW_TEXT_LENGTH) {
      context.getCounter("Token Annotation", "skip -- raw text too long (" + MAX_RAW_TEXT_LENGTH + ")")
          .increment(1);
      return;
    }


    // create put
    Put put = new Put(mapKey.get());
    boolean add = false;

    AnnotationSet parSet = DocSchema.getAnnotationSet(row, Constants.PAR);
    if (parSet == null || parSet.size() == 0) {
      context.getCounter("Token Annotation", "skip -- no paragraphs").increment(1);
      return;
    }

    AnnotationSet sentences = DocSchema.getAnnotationSet(row, Constants.SENT);
    if (sentences == null || sentences.size() == 0) {
      context.getCounter("Token Annotation", "skip -- no sentences").increment(1);
      return;
    }

    AnnotationSet toks = DocSchema.getAnnotationSet(row, Constants.TOKEN);
    if (toks == null || toks.size() == 0) {
      toks = new AnnotationSet(Constants.TOKEN);
      add = true;
      context.getCounter("Token Annotation", "add token").increment(1);

      int sentCount = 0;
      for (Annotation sent : sentences) {
        sentCount++;
        if (sentCount > 1000) {
          break;
        }
        String sentText = Annotation.getAnnotText(sent, rawText);
        int sentStart = sent.getStartOffset();
        // tokenize the sentence
        Span[] spans = tknzr.tokenizePos(sentText);

        // add each token to the annotation set
        AnnotationSet sentToks = new AnnotationSet(Constants.TOKEN);
        for (Span token : spans) {
          sentToks.add(sentStart + token.getStart(), sentStart + token.getEnd(), "token");
        }
        toks.addAll(sentToks);
        context.getCounter("Token Annotation", "add tokens for sentence").increment(1);
        addAnnotation(put, toks, Constants.TOKEN);

      }
    }

    // write output
    // add annotation text
    if (add) {
      try {
        docTable.put(put);
        docTable.flushCommits();
        context.getCounter("Token Annotation", "put").increment(1);
      }
      catch (IOException e) {
        context.getCounter("Token Annotation error", "io exception in put").increment(1);
        e.printStackTrace();
      }
      catch (IllegalArgumentException e) {
        context.getCounter("Token Annotation error", "illegal arg exception in put").increment(1);
        e.printStackTrace();
      }
    }
    else {
      context.getCounter("Token Annotation", "skip -- no columns to add").increment(1);
    }
  }
  finally {
    if (progress != null) {
      progress.interrupt();
    }
  }

}

private void addAnnotation(Put put, AnnotationSet set, String name)
{
  add(put, DocSchema.annotationsCF, name, getAnnotationStr(set));
}



}
}
