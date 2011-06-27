package reconcile.hbase.parser;

import java.util.List;
import java.util.Properties;

import edu.stanford.nlp.ling.CoreAnnotations.CharacterOffsetBeginAnnotation;
import edu.stanford.nlp.ling.CoreAnnotations.CharacterOffsetEndAnnotation;
import edu.stanford.nlp.ling.CoreAnnotations.SentencesAnnotation;
import edu.stanford.nlp.ling.CoreAnnotations.TokensAnnotation;
import edu.stanford.nlp.ling.CoreLabel;
import edu.stanford.nlp.pipeline.Annotation;
import edu.stanford.nlp.pipeline.StanfordCoreNLP;
import edu.stanford.nlp.util.CoreMap;

import reconcile.data.AnnotationSet;
import reconcile.general.Constants;


// @SuppressWarnings("rawtypes")
public class StanfordTokenParser {


// private static Pattern pWord = Pattern.compile("\\w");
// private Map<String, String> mTypeMap = Maps.newHashMap();


/**
 *
 */
public StanfordTokenParser()
{

  init();
}

private StanfordCoreNLP pipeline;

public void init()
{
  // creates a StanfordCoreNLP object, with tokenization and sentence splitting
  Properties props = new Properties();
  props.put("annotators", "tokenize, ssplit");
  pipeline = new StanfordCoreNLP(props);

}

public void parse(String text, AnnotationSet tokenSet, AnnotationSet sentenceSet)
{

  // create an empty Annotation just with the given text
  Annotation document = new Annotation(text);

  // run all Annotators on this text
  pipeline.annotate(document);

  // these are all the sentences in this document
  // a CoreMap is essentially a Map that uses class objects as keys and has values with custom types
  List<CoreMap> sentences = document.get(SentencesAnnotation.class);

  for (CoreMap sentence : sentences) {

    // traversing the words in the current sentence
    // a CoreLabel is a CoreMap with additional token-specific methods
    int sentBegin = sentence.get(CharacterOffsetBeginAnnotation.class);
    int sentEnd = sentence.get(CharacterOffsetEndAnnotation.class);
    System.out.println("sentence end: " + sentEnd);
    sentenceSet.add(sentBegin, sentEnd, Constants.SENT);

    for (CoreLabel token : sentence.get(TokensAnnotation.class)) {
      int currentBegin = token.get(CharacterOffsetBeginAnnotation.class);
      int currentEnd = token.get(CharacterOffsetEndAnnotation.class);
      tokenSet.add(currentBegin, currentEnd, Constants.TOKEN);
    }
  }
}




}
