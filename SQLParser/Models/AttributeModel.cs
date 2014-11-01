using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace prefSQL.SQLParser.Models
{
    class AttributeModel
    {

        public AttributeModel(String strColumn, String strOperator, String strTable, String strInnerTable, String strInnerColumn, String strSingleColumn, String strInnerSingleColumn, Boolean includeOthers, String strInnerColumnAccumulation)
        {
            Column = strColumn;
            Op = strOperator;
            Table = strTable;
            InnerTable = strInnerTable;
            InnerColumn = strInnerColumn;
            SingleColumn = strSingleColumn; //User for the additional OR with text values
            InnerSingleColumn = strInnerSingleColumn;
            IncludesOthers = includeOthers;
            InnerColumnAccumulation = strInnerColumnAccumulation;
        }

        public Boolean IncludesOthers { get; set; }

        public string SingleColumn { get; set; }


        public string InnerSingleColumn { get; set; }
        
        public string Column { get; set; }

        public string InnerColumn { get; set; }

        public string InnerColumnAccumulation { get; set; }

        public string InnerTable { get; set; }

        //Operator
        public string Op { get; set; }

        public string Table { get; set; }
    }
}
