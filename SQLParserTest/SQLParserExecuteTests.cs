﻿using System;
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
        public void TestSQLSelectWithCoalesce()
        {
            string sql = "select COALESCE(cars.id, 0) FROM cars SKYLINE OF cars.price LOW, cars.mileage LOW";

            SQLCommon common = new SQLCommon();
            DataTable dt = common.ParseAndExecutePrefSQL(Helper.ConnectionString, Helper.ProviderName, sql);

            Assert.IsTrue(dt.Rows.Count > 0, "Select result in no data");

        }

    }
}