package reconcile.hbase.mapreduce.annotation;

import com.google.common.base.Objects;


public class Link {

private static final double EPSILON = 0.0001;

public String leftEntity;

public String rightEntity;

public double weight;

public Link() {
}

public Link(String left, String right, double w) {
  leftEntity = left;
  rightEntity = right;
  weight = w;
}

public String getLeftEntity()
{
  return leftEntity;
}

public void setLeftEntity(String leftEntity)
{
  this.leftEntity = leftEntity;
}

public String getRightEntity()
{
  return rightEntity;
}

public void setRightEntity(String rightEntity)
{
  this.rightEntity = rightEntity;
}

public double getWeight()
{
  return weight;
}

public void setWeight(double weight)
{
  this.weight = weight;
}

@Override
public int hashCode()
{
  return Objects.hashCode(leftEntity, rightEntity, weight);
}

@Override
public boolean equals(Object o)
{
  if (o instanceof Link) {
    Link other = (Link) o;
    return Objects.equal(leftEntity, other.leftEntity) && Objects.equal(rightEntity, other.rightEntity)
        && Math.abs(weight - other.weight) < EPSILON;
  }
  return Objects.equal(this, o);
}
}
