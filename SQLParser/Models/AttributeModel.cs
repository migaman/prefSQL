using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace prefSQL.SQLParser.Models
{
    internal class AttributeModel
    {

        public AttributeModel(string strRankExpression, string strInnerColumnExpression, string strFullColumnName, string strInnerColumnName, bool isComparable, string strIncomporableAttribute, bool isCategory, string strHexagonIncomparable, int amountIncomparable, int weightHexagonIncomparable, string strExpression)
        {
            RankExpression = strRankExpression;                     //Rank expression                 (i.e. CASE WHEN colors.name = 'turquoise' THEN 0 WHEN colors.name = 'yellow' THEN 100 ELSE 200 END
            InnerExpression = strInnerColumnExpression;             //Inner column expression           (i.e CASE WHEN colors_INNER.name = 'turquoise' THEN 0 WHEN colors_INNER.name = 'yellow' THEN 100 ELSE 200 END)
            Expression = strExpression;                             //

            FullColumnName = strFullColumnName;                     //Used for the additional OR with text values (i.e. OR colors_INNER.name = colors.name)
            InnerFullColumnName = strInnerColumnName;               //Used for the additional OR with text values (i.e. OR colors_INNER.name = colors.name)
            IsCategorical = isCategory;                             //Defines if it is categorical preference (Used for the additional OR-Clause in native SQL)
            
            //Attributes for incomparability
            Comparable = isComparable;                              //Check if at least one value is incomparable
            IncomparableAttribute = strIncomporableAttribute;       //Attribute that returns the textvalue if the value is incomparable
            AmountOfIncomparables = amountIncomparable;

            //Additional Hexagon attributes
            HexagonIncomparable = strHexagonIncomparable;
            HexagonWeightIncomparable = weightHexagonIncomparable;
        }



        public string RankExpression { get; set; }
        public string InnerExpression { get; set; }
        public string Expression { get; set; }
        public string FullColumnName { get; set; }
        public string InnerFullColumnName { get; set; }
        public bool IsCategorical { get; set; }



        //Attributes for incomparability
        public int AmountOfIncomparables { get; set; }
        public bool Comparable { get; set; }
        public string IncomparableAttribute { get; set; }


        //Hexagon attributes
        public int HexagonWeightIncomparable { get; set; }
        public string HexagonIncomparable { get; set; }
    }
}
