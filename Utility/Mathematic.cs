using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Utility
{
    class Mathematic
    {
        public double getPearson(double[] x, double[] y)
        {

            //will regularize the unusual case of complete correlation
            const double TINY = 1.0e-20;
            int j, n = x.Length;
            Double yt, xt;
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
                xt = x[j] - ax;
                yt = y[j] - ay;
                sxx += xt * xt;
                syy += yt * yt;
                sxy += xt * yt;
            }
            return sxy / (Math.Sqrt(sxx * syy) + TINY);

        }


        public double getStdDerivation(List<double> numbers)
        {
            return Math.Sqrt(getVariance(numbers));
        }

        public double getStdDerivation(List<long> numbers)
        {
            return Math.Sqrt(getVariance(numbers));
        }

        //Source: http://www.remondo.net/calculate-the-variance-and-standard-deviation-in-csharp/
        //private double Variance(this IEnumerable<double> list)
        public double getVariance(List<double> numbers)
        {
            //List<double> numbers = list.ToList();

            double mean = numbers.Average(); // .Mean();
            double result = numbers.Sum(number => Math.Pow(number - mean, 2.0));

            return result / numbers.Count;
        }

        
        public double getVariance(List<long> numbers)
        {
            List<double> doubleList = numbers.ConvertAll(x => (double)x);
            return getVariance(doubleList);
        }
    }
}
