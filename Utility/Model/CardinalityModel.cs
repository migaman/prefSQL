using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Utility.Model
{
    class CardinalityModel
    {
        private string col;
        private int cardinality;

        public CardinalityModel()
        {

        }

        public CardinalityModel(string _col, int _cardinality)
        {
            col = _col;
            cardinality = _cardinality;
        }

        public int Compare(object x, object y)
        {
            if (x is CardinalityModel && y is CardinalityModel)
            {
                return Compare((CardinalityModel)x, (CardinalityModel)y);
            }
            else
            {
                return 0;
            }
        }

        public int Compare(CardinalityModel x, CardinalityModel y)
        {
            if (x.cardinality > y.cardinality)
                return -1;
            if (x.cardinality == y.cardinality)
                return 0;
            return 1;
        }





        public string Col
        {
            get { return col; }
            set { col = value; }
        }
        

        public int Cardinality
        {
            get { return cardinality; }
            set { cardinality = value; }
        }

    }
}
