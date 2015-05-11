using System.Collections;

namespace Utility.Model
{
    class CorrelationModel : IComparer
    {
        public CorrelationModel()
        {

        }

        public CorrelationModel(string colA, string colB, double correlation)
        {
            ColA = colA;
            ColB = colB;
            Correlation = correlation;
        }

        public int Compare(object x, object y)
        {
            CorrelationModel correlationModelX = x as CorrelationModel;
            CorrelationModel correlationModelY = y as CorrelationModel;
            if (correlationModelX != null && correlationModelY != null)
            {
                return Compare(correlationModelX, correlationModelY);
            }
            else
            {
                return 0;
            }

           
        }
        
        public int Compare(CorrelationModel x, CorrelationModel y)
        {
            if (x.Correlation > y.Correlation)
                return -1;
            if (x.Correlation == y.Correlation)
                return 0;
            return 1;
        }

        




        public string ColA { get; set; }


        public string ColB { get; set; }

        public double Correlation { get; set; }
    }
}
