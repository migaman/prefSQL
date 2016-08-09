using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Data.SqlClient;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using prefSQL.SQLParser;
using prefSQL.SQLParser.Models;
using prefSQL.SQLSkyline;

namespace prefSQL.SQLParserTest
{
    [TestClass]
    public class SQLParserRankingTest
    {

        private string[] GetPreferences()
        {
            string[] strPrefSQL = new string[7];

            //1 numerical preference
            strPrefSQL[0] = "SELECT t1.id AS ID, t1.title, t1.price FROM cars t1 ORDER BY WEIGHTEDSUM(t1.price LOW 1)";
            //3 numerical preferences
            strPrefSQL[1] = "SELECT * FROM cars t1 ORDER BY WEIGHTEDSUM( t1.price LOW 0.5, t1.mileage LOW 0.3, t1.horsepower HIGH 0.2)";
            //6 numerical preferences with EQUAL STEPS
            strPrefSQL[2] = "SELECT t.price,t.mileage,t.horsepower,t.enginesize,t.consumption,t.doors,colors.name,fuels.name,bodies.name,t.title,makes.name,conditions.name FROM cars t LEFT OUTER JOIN colors ON t.color_id = colors.ID LEFT OUTER JOIN fuels ON t.fuel_id = fuels.ID LEFT OUTER JOIN bodies ON t.body_id = bodies.ID LEFT OUTER JOIN makes ON t.make_id = makes.ID LEFT OUTER JOIN conditions ON t.condition_id = conditions.ID " +
                "ORDER BY WEIGHTEDSUM( " + 
                  "t.price LOW 3000 EQUAL 0.1 " +
                ", t.mileage LOW 20000 EQUAL 0.1 " +
                ", t.horsepower HIGH 20 EQUAL 0.1 " +
                ", t.enginesize HIGH 1000 EQUAL 0.1 " + 
                ", t.consumption LOW 15 EQUAL 0.1" +
                ", t.doors HIGH 0.5)";


            //Preference with TOP Keyword
            //3 numerical preferences with TOP Keyword
            strPrefSQL[3] = "SELECT TOP 55208 t1.title FROM cars t1 ORDER BY WEIGHTEDSUM( t1.price LOW 0.3, t1.mileage LOW 0.2, t1.horsepower HIGH 0.5)";

            //OTHERS EQUAL at the end
            strPrefSQL[4] = "SELECT t1.id, t1.title AS AutoTitel, t1.price, t1.mileage, colors.name FROM cars t1 LEFT OUTER JOIN colors ON t1.color_id = colors.ID ORDER BY WEIGHTEDSUM( t1.price LOW 0.92, colors.name ('red' >> 'blue' >> OTHERS EQUAL) 0.08)";
            //OTHERS EQUAL at the beginning
            strPrefSQL[5] = "SELECT t1.id, t1.title, t1.price, t1.mileage, colors.name FROM cars t1 LEFT OUTER JOIN colors ON t1.color_id = colors.ID ORDER BY WEIGHTEDSUM( t1.price LOW 0.25, colors.name (OTHERS EQUAL >> 'blue') 0.75)";
            //OTHERS EQUAL in the middle
            strPrefSQL[6] = "SELECT t1.id, t1.title, t1.price, t1.mileage, colors.name FROM cars t1 LEFT OUTER JOIN colors ON t1.color_id = colors.ID ORDER BY WEIGHTEDSUM( t1.price LOW 0.99, colors.name ('red' >> OTHERS EQUAL >> 'blue') 0.01)";

            return strPrefSQL;
        }


        /**
         * This test checks if the algorithms return the same amount of tupels for different prefSQL statements
         * At the moment it contains the BNL, nativeSQL and Hexagon algorithm. It is intendend to add the D&Q as soon
         * as it works
         * 
         * */
        [TestMethod]
        public void TestWeightedSum_DataTable()
        {
            string[] strPrefSQL = GetPreferences();

            for (int i = 0; i <= strPrefSQL.GetUpperBound(0); i++)
            {

                SQLCommon common = new SQLCommon();
                common.SkylineType = new SkylineSQL();
                DataTable dt = common.ParseAndExecutePrefSQL(Helper.ConnectionString, Helper.ProviderName, strPrefSQL[i]);
                                

                //Check tuples (every algorithm should deliver the same amount of tuples)
                Assert.AreEqual(dt.Rows.Count, 55208, 0, "WeightedSum Sorting failed");
                

            }
        }
    }
}
