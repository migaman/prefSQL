using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace prefSQL.SQLParser.Models
{
    class RankingModel
    {

        public RankingModel(string strFullColumnName, string strColumnName, string strExpression, double weight, string strSelectExtrema)
        {
            FullColumnName = strFullColumnName;
            ColumnName = strColumnName;
            Expression = strExpression;
            Weight = weight;
            SelectExtrema = strSelectExtrema;
        }

        public string SelectExtrema { get; set; }


        public double Weight { get; set; }

        public string Expression { get; set; }

        public string ColumnName { get; set; }


        public string FullColumnName { get; set; }
    }


   

}
