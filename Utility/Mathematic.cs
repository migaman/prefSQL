using System;
using System.Collections.Generic;
using System.Linq;

namespace Utility
{
    using System.CodeDom;

    class Mathematic
    {
        //http://www.codeproject.com/Articles/49723/Linear-correlation-and-statistical-functions
        public double GetPearson(double[] x, double[] y)
        {

            //will regularize the unusual case of complete correlation
            const double tiny = 1.0e-20;
            int j, n = x.Length;
            Double syy = 0.0, sxy = 0.0, sxx = 0.0, ay = 0.0, ax = 0.0;
            for (j = 0; j < n; j++)
            {
                //finds the mean
                ax += x[j];
                ay += y[j];
            }
            ax /= n;
            ay /= n;
            for (j = 0; j < n; j++)
            {
                // compute correlation coefficient
                double xt = x[j] - ax;
                double yt = y[j] - ay;
                sxx += xt * xt;
                syy += yt * yt;
                sxy += xt * yt;
            }
            return sxy / (Math.Sqrt(sxx * syy) + tiny);

        }


        public double GetStdDeviation(List<double> numbers)
        {
            return Math.Sqrt(GetVariance(numbers));
        }

        public double GetStdDeviation(List<long> numbers)
        {
            return Math.Sqrt(GetVariance(numbers));
        }

        public double GetSampleStdDeviation(List<double> numbers)
        {
            return Math.Sqrt(GetSampleVariance(numbers));
        }

        public double GetSampleStdDeviation(List<long> numbers)
        {
            return Math.Sqrt(GetSampleVariance(numbers));
        }




        //Source: http://www.remondo.net/calculate-the-variance-and-standard-deviation-in-csharp/
        //private double Variance(this IEnumerable<double> list)
        public double GetVariance(List<double> numbers)
        {            
            double mean = numbers.Average();
            double result = numbers.Sum(number => Math.Pow(number - mean, 2.0));
            return result/numbers.Count;
        }


        //Source: http://www.remondo.net/calculate-the-variance-and-standard-deviation-in-csharp/
        //private double Variance(this IEnumerable<double> list)
        public double GetSampleVariance(List<double> numbers)
        {
            double mean = numbers.Average();
            double result = numbers.Sum(number => Math.Pow(number - mean, 2.0));

            if (numbers.Count > 1)
            {
                //Use Sample variance (n-1) instead of the original variance
                return result/(numbers.Count - 1);
            } 
            else
            {
                return 0;

            }
        }

        
        public double GetVariance(List<long> numbers)
        {
            List<double> doubleList = numbers.ConvertAll(x => (double)x);
            return GetVariance(doubleList);
        }

        public double GetSampleVariance(List<long> numbers)
        {
            List<double> doubleList = numbers.ConvertAll(x => (double)x);
            return GetSampleVariance(doubleList);
        }

        /// <summary>
        ///     TODO: from http://www.remondo.net/calculate-mean-median-mode-averages-csharp/
        /// </summary>
        /// <param name="list"></param>
        /// <returns></returns>
        public static T Median<T>(IEnumerable<T> list)
        {
            int midIndex;
            return Median(list, out midIndex);
        }

        /// <summary>
        ///     TODO: based on http://www.remondo.net/calculate-mean-median-mode-averages-csharp/
        /// </summary>
        /// <param name="list"></param>
        /// <param name="midIndex"></param>
        /// <returns></returns>
        public static T Median<T>(IEnumerable<T> list, out int midIndex)
        {
            MathProvider<T> mathP;

            if (typeof(T) == typeof(double))
                mathP = new DoubleMathProvider() as MathProvider<T>;
            else if (typeof (T) == typeof (long))
                mathP = new LongMathProvider() as MathProvider<T>;
            else
            {
                throw new Exception("type not supported: "+typeof(T));
            }

            List<T> orderedList = list.OrderBy(numbers => numbers).ToList();

            int listSize = orderedList.Count;
            T result;

            midIndex = listSize / 2;

            if (listSize % 2 == 0) // even
            {
                result = mathP.Half(mathP.Add(orderedList.ElementAt(midIndex - 1), orderedList.ElementAt(midIndex)));
            }
            else // odd
            {
                result = orderedList.ElementAt(midIndex);
            }

            return result;
        }

        /// <summary>
        /// TODO: http://en.wikipedia.org/wiki/Quartile#Method_1
        /// </summary>
        /// <param name="list"></param>
        /// <returns></returns>
        public static T LowerQuartile<T>(IEnumerable<T> list)
        {
            List<T> orderedList = list.OrderBy(numbers => numbers).ToList();

            int takeElements;
            Median(orderedList, out takeElements);

            return Median(orderedList.Take(takeElements));
        }

        /// <summary>
        /// TODO: http://en.wikipedia.org/wiki/Quartile#Method_1
        /// </summary>
        /// <param name="list"></param>
        /// <returns></returns>
        public static T UpperQuartile<T>(IEnumerable<T> list)
        {
            List<T> orderedList = list.OrderBy(numbers => numbers).ToList();

            int skipElements;
            Median(orderedList, out skipElements);

            if (orderedList.Count % 2 == 1)
            {
                skipElements++;
            }

            return Median(orderedList.Skip(skipElements));
        }

        /// <summary>
        /// TODO: based on http://stackoverflow.com/a/64142/2406389
        /// </summary>
        /// <typeparam name="T"></typeparam>
        private abstract class MathProvider<T>
        {
            public abstract T Add(T a, T b);
            public abstract T Half(T a);
        }

        private class DoubleMathProvider : MathProvider<double>
        {
            public override double Half(double a)
            {
                return a / 2;
            }
            public override double Add(double a, double b)
            {
                return a + b;
            }
        }

          private class LongMathProvider : MathProvider<long>
        {
              public override long Half(long a)
              {
                  return a / 2;
              }
              public override long Add(long a, long b)
              {
                  return a + b;
              }
        }
    }
}

