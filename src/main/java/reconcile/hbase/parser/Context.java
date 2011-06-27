package reconcile.hbase.parser;

import java.util.Map;

import com.google.common.collect.Maps;


public class Context {

Map<String, Map<String, Counter>> map;

public Context() {
  map = Maps.newTreeMap();
}
public Counter getCounter(String family, String name)
{
  Map<String, Counter> m = map.get(family);
  if (m == null) {
    m = Maps.newTreeMap();
    map.put(family, m);
  }
  Counter c = m.get(name);
  if (c == null) {
    c = new Counter();
    m.put(name, c);
  }
  return c;
}

@edu.umd.cs.findbugs.annotations.SuppressWarnings(value = "WMI_WRONG_MAP_ITERATOR")
public void print()
{
  for (String family : map.keySet()) {
    Map<String, Counter> m = map.get(family);
    for (String name : m.keySet()) {
      System.out.println(family + "\t" + name + "\t" + m.get(name).count);
    }
  }
}

}
