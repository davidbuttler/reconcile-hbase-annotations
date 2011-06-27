package trinidad.document.tokenizer;

import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import gov.llnl.text.util.MapUtil;


public class RegexTokenizer
    extends Tokenizer {

protected static final Pattern pWord = Pattern.compile("\\w+");

protected static final Pattern pLetter = Pattern.compile(".*[a-z]+.*");

@Override
public Map<String, Integer> getDocumentTerms(String text)
{
  Map<String, Integer> terms = Maps.newHashMap();
  if (text == null || text.length() == 0) return terms;
  Matcher matcher = pWord.matcher(text);
  while (matcher.find()) {
    String token = text.substring(matcher.start(), matcher.end());
    token = token.toLowerCase();
    if (token.length() > 2 && pLetter.matcher(token).matches()) {
      MapUtil.addCount(terms, token);
    }
  }
  return terms;
}

@Override
public List<String> getDocumentTermList(String text)
{
  List<String> terms = Lists.newArrayList();
  if (text == null || text.length() == 0) return terms;
  Matcher matcher = pWord.matcher(text);
  while (matcher.find()) {
    String token = text.substring(matcher.start(), matcher.end());
    token = token.toLowerCase();
    if (token.length() > 2 && pLetter.matcher(token).matches()) {
      terms.add(token);
    }
  }
  return terms;
}

}
