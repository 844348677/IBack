import breeze.numerics.{exp, log}
import breeze.stats.MeanAndVariance

/**
  * Created by hdd on 5/4/16.
  */
object BreezeStart {
  def main(args: Array[String]) {
    import breeze.linalg._

    //密集矩阵，稀疏矩阵使用SparseVector.zeros[Double](5)，
    val x = DenseVector.zeros[Double](5)
    println(x)
    //所有矩阵都是列矩阵， 如果使用使用row矩阵 Transpose[Vector[T]]
    println(x(0))
    x(1)=2
    println(x)
    //x(i) == x(x.length + i)

    x(3 to 4)  := 0.5
    println(x)
    //分片操作使用向量化的操作符 :=
    x(0 to 1) := DenseVector(.1,.2)
    println(x)

    //矩阵
    val m  = DenseMatrix.zeros[Int](5,5)
    println(m)
    println((m.rows,m.cols))

    println(m(::,1))
    m(4,::) := DenseVector(1,2,3,4,5).t //transpose to match row shape

    println(m)

    //m := x
    //m := DenseMatrix.zeros[Int](3,3)

    m(0 to 1,0 to 1) := DenseMatrix((3,1),(-1,-2))
    println(m)
    /*
    Operation 	Breeze 	Matlab 	Numpy
    Elementwise addition 	a + b 	a + b 	a + b
    Elementwise multiplication 	a :* b 	a .* b 	a * b
    Elementwise comparison 	a :< b 	a < b (gives matrix of 1/0 instead of true/false) 	a < b
    Inplace addition 	a :+= 1.0 	a += 1 	a += 1
    Inplace elementwise multiplication 	a :*= 2.0 	a *= 2 	a *= 2
    Vector dot product 	a dot b,a.t * b† 	dot(a,b) 	dot(a,b)
    Elementwise sum 	sum(a) 	sum(sum(a)) 	a.sum()
    Elementwise max 	a.max 	max(a) 	a.max()
    Elementwise argmax 	argmax(a) 	argmax(a) 	a.argmax()
    Ceiling 	ceil(a) 	ceil(a) 	ceil(a)
    Floor 	floor(a) 	floor(a) 	floor(a)
     */
    //Adapting a matrix so that operations can be applied column-wise or row-wise is called broadcasting
    val dm = DenseMatrix((1.0,2.0,3.0),(4.0,5.0,6.0))
    val res = dm(::,*) + DenseVector(3.0,4.0)
    println(res)

    res(::,*) := DenseVector(3.0,4.0)
    println(res)

    import breeze.stats.mean
    println(mean(dm(*,::)))

    import breeze.stats.distributions._
    val poi = new Poisson(3.0) //泊松分布
    val s = poi.sample(5)
    println(s)

    println(s map {poi.probabilityOf(_)})

    val doublePoi = for(x <- poi) yield x.toDouble
    //doublePoi.foreach(println)

    println(breeze.stats.meanAndVariance(doublePoi.samples.take(1000)))
    println((poi.mean,poi.variance))

    val expo = new Exponential(0.5)
    println(expo.rate)

    expo.probability(0,log(2)*expo.rate)
    println(expo.probability(0.0,1.5))

    println(1-exp(-3.0))

    val samples = expo.sample(2).sorted
    println(samples)
    println(expo.probability(samples(0),samples(1)))
    println(breeze.stats.meanAndVariance(expo.samples.take(10000)))
    println((1/expo.rate,1/(expo.rate*expo.rate)))

    import breeze.optimize._
    val f = new DiffFunction[DenseVector[Double]] {
      override def calculate(x: DenseVector[Double]): (Double, DenseVector[Double]) = {
        (norm((x-3d) :^ 2d,1d),(x * 2d) - 6d)
      }
    }
    println(f.valueAt(DenseVector(3,3,3)))
    println(f.gradientAt(DenseVector(3,0,1)))
    println(f.calculate(DenseVector(0,0)))

    def g(x:DenseVector[Double]) = (x - 3.0) :^ 2.0 sum

    println(g(DenseVector(0.,0.,0.)))
/*    val diffg = new ApproximateGradientFunction(g)
    println(diffg.gradientAt(DenseVector(3,0,1)))  方法接收一个Nothing！！！ */

    val lbfgs = new LBFGS[DenseVector[Double]](maxIter=100,m=3)

    val optimum = lbfgs.minimize(f,DenseVector(0,0,0))

    println(optimum)
    println(f(optimum))


  }
}
