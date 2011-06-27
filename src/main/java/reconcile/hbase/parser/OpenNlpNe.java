package reconcile.hbase.parser;

import java.io.DataInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;
import java.util.zip.GZIPInputStream;

import opennlp.maxent.MaxentModel;
import opennlp.maxent.io.BinaryGISModelReader;
import opennlp.tools.namefind.NameFinderME;
import opennlp.tools.tokenize.TokenizerME;
import opennlp.tools.util.Span;

import gov.llnl.text.util.FileUtils;

import reconcile.data.Annotation;
import reconcile.data.AnnotationSet;
import reconcile.featureExtractor.ParagraphSplitter;
import reconcile.featureExtractor.SentenceSplitterOpenNLP;
import reconcile.general.Constants;

public class OpenNlpNe {

private TokenizerME tknzr;

private NameFinderME[] finders;

String[] tagTypes = { "date", "location", "money", "organization", "percentage", "person", "time" };

private SentenceSplitterOpenNLP sentenceSplitter;

private ParagraphSplitter paragraphSplitter;

private static final String OPEN_NLP_TOKENIZER_MODEL = "OpenNLP/models/EnglishTok.bin.gz";

public OpenNlpNe() {
  try {
    System.out.println("load par");
    paragraphSplitter = new ParagraphSplitter();
    System.out.println("load sent");
    sentenceSplitter = new SentenceSplitterOpenNLP();

    // set up the tokenizer
    System.out.println("load tokens");
    InputStream tokenResStream = this.getClass().getClassLoader().getResourceAsStream(OPEN_NLP_TOKENIZER_MODEL);
    DataInputStream dis = new DataInputStream(new GZIPInputStream(tokenResStream));
    tknzr = new TokenizerME((new BinaryGISModelReader(dis)).getModel());

    /* Establish a name finder for each tag type */
    finders = new NameFinderME[tagTypes.length];

    for (int i = 0; i < tagTypes.length; ++i) {
      System.out.println("load ne: " + tagTypes[i]);

      InputStream resStream = this.getClass().getClassLoader().getResourceAsStream(
          "OpenNLP/models/" + tagTypes[i] + ".bin.gz");
      // load the model
      MaxentModel model = getModel(resStream);

      // create the finder
      finders[i] = new NameFinderME(model);
    }

  }
  catch (Exception e) {
    e.printStackTrace();
    throw new RuntimeException(e);
  }

}

private static MaxentModel getModel(InputStream resStream)
{
  try {
    DataInputStream dis = new DataInputStream(new GZIPInputStream(resStream));
    return (new BinaryGISModelReader(dis)).getModel();
  }
  catch (IOException e) {
    e.printStackTrace();
    return null;
  }
}

public static void main(String[] args)
{
  try {
    OpenNlpNe nlp = new OpenNlpNe();
    nlp.parse(args[0]);
  }
  catch (IOException e) {
    e.printStackTrace();
  }
}

private void parse(String fileName)
    throws IOException
{
  String rawText = FileUtils.readFile(new File(fileName));

  AnnotationSet parSet = paragraphSplitter.parse(rawText, Constants.PAR);

  AnnotationSet sentences = sentenceSplitter.parse(rawText, parSet, Constants.SENT);

  AnnotationSet toks = new AnnotationSet(Constants.TOKEN);
    for (Annotation sent : sentences) {
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
    }


  // now parse out the named entities
  AnnotationSet namedEntities = getNEs(rawText, sentences, toks);

  for (Annotation a : namedEntities) {
    // for (Annotation a : toks) {
    System.out.println("type: " + a.getType() + "(" + a.getStartOffset() + "," + a.getEndOffset() + "):"
        + Annotation.getAnnotText(a, rawText));
  }
}

private AnnotationSet getNEs(String text, AnnotationSet sentSet, AnnotationSet tokSet)
{
  AnnotationSet namedEntities = new AnnotationSet(Constants.NE);

  Iterator<Annotation> sents = sentSet.iterator();
  while (sents.hasNext()) {
    Annotation sent = (sents.next());

    int sentStart = sent.getStartOffset();
    int sentEnd = sent.getEndOffset();

    String sentence = text.substring(sentStart, sentEnd);
    System.out.println("Sentence: " + sentence);
    // tokenize the sentence
    Span[] spans = tknzr.tokenizePos(sentence);

    // add each token to the annotation set
    String[] tokenList = new String[spans.length];
    for (int i = 0; i < spans.length; i++) {
      tokenList[i] = spans[i].getCoveredText(sentence);
      // System.out.println(tokenList[i]);
    }

    System.out.println("total number of tokens: " + tokenList.length);
    for (int fndIndx = 0; fndIndx < finders.length; ++fndIndx) {
      NameFinderME findr = finders[fndIndx];

      // Tag the sentence
      Span[] sentTags = findr.find(tokenList); // findr.find(tokenList, new HashMap());
      String[] textTokens = Span.spansToStrings(sentTags, tokenList);

      for (String token : textTokens) {
        System.out.println("ne " + tagTypes[fndIndx] + ": " + token);
        // if (span.getStart() == 0 && span.getEnd() == tokenList.length) {
        // continue;
        // }
        // System.out.println("span:" + span);
        // namedEntities.add(sentStart + spans[span.getStart()].getStart(), sentStart + spans[span.getEnd()].getEnd(),
        // tagTypes[fndIndx]);
      }
    }
  }

  return namedEntities;
}

}
