using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Utility.Model
{
    class CorrelationModel : IComparer
    {
        private string colA;
        private string colB;
        private double correlation;

        public CorrelationModel()
        {

        }

        public CorrelationModel(string _colA, string _colB, double _correlation)
        {
            colA = _colA;
            colB = _colB;
            correlation = _correlation;
        }

        public int Compare(object x, object y)
        {
            if (x is CorrelationModel && y is CorrelationModel)
            {
                return Compare((CorrelationModel)x, (CorrelationModel)y);
            }
            else
            {
                return 0;
            }
        }
        
        public int Compare(CorrelationModel x, CorrelationModel y)
        {
            if (x.correlation > y.correlation)
                return -1;
            if (x.correlation == y.correlation)
                return 0;
            return 1;
        }

        




        public string ColA
        {
            get { return colA; }
            set { colA = value; }
        }
        

        public string ColB
        {
            get { return colB; }
            set { colB = value; }
        }

        public double Correlation
        {
            get { return correlation; }
            set { correlation = value; }
        }

    }
}
