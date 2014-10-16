using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace prefSQL.SQLParser.Models
{
    class AttributeModel
    {

        public AttributeModel(String strColumn, String strOperator)
        {
            Column = strColumn;
            Op = strOperator;
        }


        public string Column { get; set; }

        //Operator
        public string Op { get; set; }
    }
}
