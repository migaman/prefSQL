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
    public class SQLParserExecuteTests
    {
        [TestMethod]
        public void TestSQLSelect()
        {
            string sql = "SELECT cars.id FROM cars";

            SQLCommon common = new SQLCommon();
            DataTable dt = common.ParseAndExecutePrefSQL(Helper.ConnectionString, Helper.ProviderName, sql);

            Assert.IsTrue(dt.Rows.Count > 0, "Select result in no data");

        }



        [TestMethod]
        public void TestSQLSelectAlias()
        {
            string sql = "SELECT t.id FROM cars t";

            SQLCommon common = new SQLCommon();
            DataTable dt = common.ParseAndExecutePrefSQL(Helper.ConnectionString, Helper.ProviderName, sql);

            Assert.IsTrue(dt.Rows.Count > 0, "Select result in no data");

        }


        [TestMethod]
        public void TestSQLSelectSubquery()
        {
            string sql = "select t.id FROM (SELECT * FROM cars) t";

            SQLCommon common = new SQLCommon();
            DataTable dt = common.ParseAndExecutePrefSQL(Helper.ConnectionString, Helper.ProviderName, sql);

            Assert.IsTrue(dt.Rows.Count > 0, "Select result in no data");

        }


        [TestMethod]
        public void TestSQLWithGroupBy()
        {
            string sql = "SELECT cars.color_id FROM cars GROUP BY cars.color_id";

            SQLCommon common = new SQLCommon();
            DataTable dt = common.ParseAndExecutePrefSQL(Helper.ConnectionString, Helper.ProviderName, sql);

            Assert.IsTrue(dt.Rows.Count > 0, "Select result in no data");

        }


        [TestMethod]
        public void TestSQLSelectSubqueryWithGroupBy()
        {
            string sql = "SELECT t.color_id, Amount FROM (SELECT cars.color_id, COUNT(*) AS Amount FROM cars GROUP BY cars.color_id) t";

            SQLCommon common = new SQLCommon();
            DataTable dt = common.ParseAndExecutePrefSQL(Helper.ConnectionString, Helper.ProviderName, sql);

            Assert.IsTrue(dt.Rows.Count > 0, "Select result in no data");

        }


        [TestMethod]
        public void TestSQLSelectWithCoalesce()
        {
            string sql = "select COALESCE(cars.id, 0) FROM cars SKYLINE OF cars.price LOW, cars.mileage LOW";

            SQLCommon common = new SQLCommon();
            DataTable dt = common.ParseAndExecutePrefSQL(Helper.ConnectionString, Helper.ProviderName, sql);

            Assert.IsTrue(dt.Rows.Count > 0, "Select result in no data");

        }


        [TestMethod]
        public void TestSQLSkylineWithFavour()
        {
            string sql = "SELECT cars.title, colors.name FROM cars LEFT OUTER JOIN colors ON cars.color_id = colors.ID SKYLINE OF colors.name FAVOUR 'red', cars.price LOW";

            SQLCommon common = new SQLCommon();
            DataTable dt = common.ParseAndExecutePrefSQL(Helper.ConnectionString, Helper.ProviderName, sql);

            Assert.IsTrue(dt.Rows.Count > 0, "Select result in no data");

        }


        [TestMethod]
        public void TestSQLSkylineWithDisfavour()
        {
            string sql = "SELECT cars.title, colors.name FROM cars LEFT OUTER JOIN colors ON cars.color_id = colors.ID SKYLINE OF colors.name DISFAVOUR 'red', cars.price LOW";

            SQLCommon common = new SQLCommon();
            DataTable dt = common.ParseAndExecutePrefSQL(Helper.ConnectionString, Helper.ProviderName, sql);

            Assert.IsTrue(dt.Rows.Count > 0, "Select result in no data");

        }

        

    }
}
