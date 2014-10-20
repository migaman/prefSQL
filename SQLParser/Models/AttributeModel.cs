using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace prefSQL.SQLParser.Models
{
    class AttributeModel
    {

        public AttributeModel(String strColumn, String strOperator, String strTable, String strInnerTable, String strInnerColumn)
        {
            Column = strColumn;
            Op = strOperator;
            Table = strTable;
            InnerTable = strInnerTable;
            InnerColumn = strInnerColumn;
        }

        
        public string Column { get; set; }

        public string InnerColumn { get; set; }

        public string InnerTable { get; set; }

        //Operator
        public string Op { get; set; }

        public string Table { get; set; }
    }
}
