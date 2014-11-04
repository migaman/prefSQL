using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Utility
{
    class Program
    {
        static void Main(string[] args)
        {
            /*Demo d = new Demo();
            d.generateDemoQueries();*/

            Performance p = new Performance();
            p.GeneratePerformanceQueries(prefSQL.SQLParser.SQLCommon.Algorithm.NativeSQL);
        }
    }
}
