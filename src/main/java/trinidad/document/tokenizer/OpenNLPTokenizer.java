package trinidad.document.tokenizer;

import java.io.DataInputStream;
import java.io.InputStream;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.regex.Pattern;
import java.util.zip.GZIPInputStream;

import opennlp.maxent.io.BinaryGISModelReader;
import opennlp.tools.sentdetect.SentenceDetector;
import opennlp.tools.sentdetect.SentenceDetectorME;
import opennlp.tools.tokenize.TokenizerME;
import opennlp.tools.util.Span;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import gov.llnl.text.util.MapUtil;

import reconcile.data.Annotation;
import reconcile.data.AnnotationSet;
import reconcile.features.properties.SentNum;
import reconcile.general.Constants;

public class OpenNLPTokenizer
    extends Tokenizer {

private static final String OPEN_NLP_SENTENCE_MODEL = "OpenNLP/models/EnglishSD.bin.gz";

private static final String OPEN_NLP_TOKENIZER_MODEL = "OpenNLP/models/EnglishTok.bin.gz";

protected static final Pattern pWord = Pattern.compile("\\w+");

protected static final Pattern pLetter = Pattern.compile(".*[a-z]+.*");


private TokenizerME tknzr;

private SentenceDetector sdetector;

public OpenNLPTokenizer() {
  try {
    InputStream resStream = this.getClass().getClassLoader().getResourceAsStream(OPEN_NLP_SENTENCE_MODEL);
    DataInputStream dis = new DataInputStream(new GZIPInputStream(resStream));
    sdetector = new SentenceDetectorME((new BinaryGISModelReader(dis)).getModel());

    // set up the tokenizer
    resStream = this.getClass().getClassLoader().getResourceAsStream(OPEN_NLP_TOKENIZER_MODEL);
    dis = new DataInputStream(new GZIPInputStream(resStream));

    tknzr = null;
    tknzr = new TokenizerME((new BinaryGISModelReader(dis)).getModel());
  }
  catch (Exception e) {
    e.printStackTrace();
    throw new RuntimeException();
  }
}
@Override
public Map<String, Integer> getDocumentTerms(String text)
{
  Map<String, Integer> terms = Maps.newHashMap();
  if (text == null || text.length() == 0) return terms;
  AnnotationSet sents = parseSentences(text);

  AnnotationSet tokens = parseTokens(sents, text);
  for (Annotation a : tokens) {
    MapUtil.addCount(terms, Annotation.getAnnotText(a, text));
  }
  return terms;
}

@Override
public List<String> getDocumentTermList(String text)
{
  List<String> terms = Lists.newArrayList();
  if (text == null || text.length() == 0) return terms;
  AnnotationSet sents = parseSentences(text);

  AnnotationSet tokens = parseTokens(sents, text);
  for (Annotation a : tokens) {
    terms.add(Annotation.getAnnotText(a, text));
  }
  return terms;
}

private AnnotationSet parseTokens(AnnotationSet sents, String text)
{
  AnnotationSet tokens = new AnnotationSet(Constants.TOKEN);
  for (Annotation s : sents) {
    parseSentence(tokens, s.getStartOffset(), Annotation.getAnnotText(s, text));
  }
  return tokens;
}

public void parseSentence(AnnotationSet tokens, int sentStart, String sentence)
{
  Span[] spans = parseSentence(sentence);

  // add each token to the annotation set
  for (Span token : spans) {
    tokens.add(sentStart + token.getStart(), sentStart + token.getEnd(), "token");
  }
}

public Span[] parseSentence(String sentence)
{

  // tokenize the sentence
  Span[] spans = tknzr.tokenizePos(sentence);
  return spans;
}

public AnnotationSet parseSentences(String text)
{
  AnnotationSet sent = new AnnotationSet(Constants.SENT);

  int counter = 0;

  String parText = text;
  int start = 0;
  int endOffset = text.length();
  int prevEndPos = start;
  boolean sentenceAdded = false;
  for (int e : sdetector.sentPosDetect(parText)) {
    sentenceAdded = true;
    int endPos = start + e;
    Map<String, String> feat = new TreeMap<String, String>();
    feat.put(SentNum.NAME, String.valueOf(counter++));
    // add each sentence to the annotation set
    sent.add(prevEndPos, endPos, "sentence_split", feat);

    prevEndPos = endPos;
  }
  if (!sentenceAdded) {
    Map<String, String> feat = new TreeMap<String, String>();
    feat.put(SentNum.NAME, String.valueOf(counter++));
    // add each sentence to the annotation set
    sent.add(start, endOffset, "sentence_split", feat);
  }
  else if (prevEndPos < endOffset - 1) {
    Map<String, String> feat = new TreeMap<String, String>();
    feat.put(SentNum.NAME, String.valueOf(counter++));
    // add each sentence to the annotation set
    sent.add(prevEndPos, endOffset, "sentence_split", feat);
  }

  return sent;
}

}
