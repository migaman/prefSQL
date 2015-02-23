using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace prefSQL.SQLParser.Models
{
    class AttributeModel
    {

        public AttributeModel(string strColumnExpression, string strOperator, string strInnerColumnExpression, string strFullColumnName, string strInnerColumnName, bool isComparable, string strIncomporableAttribute, string strRankColumnName, string strExpression, string strRankHexagon, string strOrderBy, bool isCategory, string columnName, string strHexagonIncomparable, int amountIncomparable, int weightHexagonIncomparable)
        {
            ColumnExpression = strColumnExpression;                 //Column expression                 (i.e. CASE WHEN colors.name = 'türkis' THEN 0 WHEN colors.name = 'gelb' THEN 100 ELSE 200 END)
            InnerColumnExpression = strInnerColumnExpression;       //Inner column expression           (i.e CASE WHEN colors_INNER.name = 'türkis' THEN 0 WHEN colors_INNER.name = 'gelb' THEN 100 ELSE 200 END)
            Op = strOperator;                                       //Operator                          (<, >)
            FullColumnName = strFullColumnName;                     //Used for the additional OR with text values (i.e. OR colors_INNER.name = colors.name)
            InnerColumnName = strInnerColumnName;                   //Dito
            Comparable = isComparable;                              //Check if at least one value is incomparable
            IncomparableAttribute = strIncomporableAttribute;       //Attribute that returns the textvalue if the value is incomparable

            Expression = strExpression;
            RankColumnName = strRankColumnName;
            RankHexagon = strRankHexagon;
            HexagonIncomparable = strHexagonIncomparable;

            OrderBy = strOrderBy;
            IsCategory = isCategory;
            ColumnName = columnName;

            AmountOfIncomparables = amountIncomparable;
            WeightHexagonIncomparable = weightHexagonIncomparable;
        }

        public AttributeModel(string strColumnExpression, string strOperator, string strInnerColumnExpression, string strFullColumnName, string strInnerColumnName, bool isComparable, string strIncomporableAttribute, string strRankColumnName, string strExpression, string strRankHexagon, string strOrderBy, bool isCategory, string columnName, string strHexagonIncomparable, int amountIncomparable)
            : this(strColumnExpression, strOperator, strInnerColumnExpression, strFullColumnName, strInnerColumnName, isComparable, strIncomporableAttribute, strRankColumnName, strExpression, strRankHexagon, strOrderBy, isCategory, columnName, strHexagonIncomparable, amountIncomparable, 0)
        {
        }


        public int AmountOfIncomparables { get; set; }
        public string FullColumnName { get; set; }

        public string InnerColumnName { get; set; }

        public string ColumnExpression { get; set; }

        public string InnerColumnExpression { get; set; }

        //Operator
        public string Op { get; set; }

        public bool Comparable { get; set; }

        public string IncomparableAttribute { get; set; }

        public string RankColumnName { get; set; }
        public string Expression { get; set; }
        
        public string RankHexagon { get; set; }

        public string HexagonIncomparable { get; set; }

        public string OrderBy { get; set; }

        public bool IsCategory { get; set; }

        public string ColumnName { get; set; }

        public int WeightHexagonIncomparable { get; set; }
    }
}
