using System;
using System.Collections.Generic;
using System.Linq;

namespace Utility
{
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

        //Source: http://www.remondo.net/calculate-the-variance-and-standard-deviation-in-csharp/
        //private double Variance(this IEnumerable<double> list)
        public double GetVariance(List<double> numbers)
        {
            //List<double> numbers = list.ToList();
            
            double mean = numbers.Average(); // .Mean();
            double result = numbers.Sum(number => Math.Pow(number - mean, 2.0));

            //Use Sample variance (n-1) instead of the original variance
            if (numbers.Count > 1)
            {
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
    }
}
