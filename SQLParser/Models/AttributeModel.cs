using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace prefSQL.SQLParser.Models
{
    class AttributeModel
    {

        public AttributeModel(String strColumnExpression, String strOperator, String strTable, String strInnerTable, String strInnerColumnExpression, String strColumnName, String strInnerColumnName)
        {
            Table = strTable;                                       //Tablename for the mainquery       (i.e. cars)
            InnerTable = strInnerTable;                             //Tablename for the subquery        (i.e. cars_INNER)
            ColumnExpression = strColumnExpression;                 //Column expression                 (i.e. CASE WHEN colors.name = 'türkis' THEN 0 WHEN colors.name = 'gelb' THEN 100 ELSE 200 END)
            InnerColumnExpression = strInnerColumnExpression;       //Inner column expression           (i.e CASE WHEN colors_INNER.name = 'türkis' THEN 0 WHEN colors_INNER.name = 'gelb' THEN 100 ELSE 200 END)
            Op = strOperator;                                       //Operator                          (<, >)
            ColumnName = strColumnName;                             //Used for the additional OR with text values (i.e. OR colors_INNER.name = colors.name)
            InnerColumnName = strInnerColumnName;                   //Dito
        }


        public string ColumnName { get; set; }

        public string InnerColumnName { get; set; }
        
        public string ColumnExpression { get; set; }

        public string InnerColumnExpression { get; set; }


        public string InnerTable { get; set; }

        //Operator
        public string Op { get; set; }

        public string Table { get; set; }
    }
}
