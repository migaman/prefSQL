using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using prefSQL.SQLParser;
using System.Data.SqlClient;

namespace prefSQL.SQLParserTest
{
    [TestClass]
    public class SQLParserSkylineTest
    {
        private const string strConnection = "Data Source=localhost;Initial Catalog=eCommerce;Integrated Security=True";

        [TestMethod]
        public void TestSKYLINEShowSkylineAttributes()
        {
            string strPrefSQL = "SELECT * FROM cars PREFERENCE LOW cars.price AND LOW cars.mileage AND HIGH cars.horsepower ";

            string expected = "SELECT * , cars.price AS SkylineAttribute0, cars.mileage AS SkylineAttribute1, cars.horsepower*-1 AS SkylineAttribute2 FROM cars WHERE NOT EXISTS(SELECT * , cars_INNER.price AS SkylineAttribute0, cars_INNER.mileage AS SkylineAttribute1, cars_INNER.horsepower*-1 AS SkylineAttribute2 FROM cars cars_INNER WHERE cars_INNER.price <= cars.price AND cars_INNER.mileage <= cars.mileage AND cars_INNER.horsepower >= cars.horsepower AND ( cars_INNER.price < cars.price OR cars_INNER.mileage < cars.mileage OR cars_INNER.horsepower > cars.horsepower) )  ORDER BY price ASC, mileage ASC, horsepower DESC";
            SQLCommon common = new SQLCommon();
            common.ShowSkylineAttributes = true;
            string actual = common.parsePreferenceSQL(strPrefSQL);

            Assert.AreEqual(expected, actual, true, "SQL not built correctly");
        }



        [TestMethod]
        public void TestSKYLINEBNLDateField()
        {

            string strPrefSQL = "SELECT cars_small.price,cars_small.mileage,cars_small.registration FROM cars_small PREFERENCE LOW cars_small.price AND LOW cars_small.mileage AND HIGHDATE cars_small.registration";

            SQLCommon common = new SQLCommon();
            common.SkylineType = SQLCommon.Algorithm.BNL;
            string sqlBNL = common.parsePreferenceSQL(strPrefSQL);

            int amountOfTupelsBNL = 0;

            SqlConnection cnnSQL = new SqlConnection(strConnection);
            try
            {
                cnnSQL.Open();

                 //BNL
                SqlCommand sqlCommand = new SqlCommand(sqlBNL, cnnSQL);
                SqlDataReader  sqlReader = sqlCommand.ExecuteReader();

                if (sqlReader.HasRows)
                {
                    while (sqlReader.Read())
                    {
                        amountOfTupelsBNL++;
                    }
                }
                cnnSQL.Close();
            }
            catch (Exception ex)
            {
                Assert.Fail("Connection failed:" + ex.Message);
            }

            Assert.AreEqual(24, amountOfTupelsBNL, 0, "Amount of tupels does not match");
        }


        [TestMethod]
        public void TestSKYLINEAmountsOfTupels11Equal()
        {

            string strPrefSQL = "SELECT cars_small.price,cars_small.mileage,cars_small.horsepower,cars_small.enginesize,cars_small.consumption,cars_small.doors,colors.name,fuels.name,bodies.name,cars_small.title,makes.name,conditions.name FROM cars_small LEFT OUTER JOIN colors ON cars_small.color_id = colors.ID LEFT OUTER JOIN fuels ON cars_small.fuel_id = fuels.ID LEFT OUTER JOIN bodies ON cars_small.body_id = bodies.ID LEFT OUTER JOIN makes ON cars_small.make_id = makes.ID LEFT OUTER JOIN conditions ON cars_small.condition_id = conditions.ID PREFERENCE LOW cars_small.price AND LOW cars_small.mileage AND HIGH cars_small.horsepower AND HIGH cars_small.enginesize AND LOW cars_small.consumption AND HIGH cars_small.doors AND HIGH colors.name {'rot' == 'blau' >> OTHERSEQUAL >> 'grau'} AND HIGH fuels.name {'Benzin' >> OTHERSEQUAL >> 'Diesel'} AND HIGH bodies.name {'Kleinwagen' >> 'Bus' >> 'Kombi' >> 'Roller' >> OTHERSEQUAL >> 'Pick-Up'} AND HIGH cars_small.title {'MERCEDES-BENZ SL 600' >> OTHERSEQUAL} AND HIGH makes.name {'ASTON MARTIN' >> 'VW' == 'Audi' >> OTHERSEQUAL >> 'FERRARI'} AND HIGH conditions.name {'Neu' >> OTHERSEQUAL}";

            SQLCommon common = new SQLCommon();
            common.SkylineType = SQLCommon.Algorithm.NativeSQL;
            string sqlNative = common.parsePreferenceSQL(strPrefSQL);
            common.SkylineType = SQLCommon.Algorithm.BNL;
            string sqlBNL = common.parsePreferenceSQL(strPrefSQL);

            int amountOfTupelsNative = 0;
            int amountOfTupelsBNL = 0;

            SqlConnection cnnSQL = new SqlConnection(strConnection);
            try
            {
                cnnSQL.Open();

                //Native
                SqlCommand sqlCommand = new SqlCommand(sqlNative, cnnSQL);
                SqlDataReader sqlReader = sqlCommand.ExecuteReader();

                if (sqlReader.HasRows)
                {
                    while (sqlReader.Read())
                    {
                        amountOfTupelsNative++;
                    }
                }
                sqlReader.Close();

                //BNL
                sqlCommand = new SqlCommand(sqlBNL, cnnSQL);
                sqlReader = sqlCommand.ExecuteReader();

                if (sqlReader.HasRows)
                {
                    while (sqlReader.Read())
                    {
                        amountOfTupelsBNL++;
                    }
                }
                cnnSQL.Close();
            }
            catch (Exception ex)
            {
                Assert.Fail("Connection failed:" + ex.Message);
            }

            Assert.AreEqual(amountOfTupelsNative, amountOfTupelsBNL, 0, "Amount of tupels does not match");
        }



        [TestMethod]
        public void TestSKYLINEAmountsOfTupels2Incomparable()
        {

            string strPrefSQL = "SELECT cars_small.price,colors.name FROM cars_small "+
            "LEFT OUTER JOIN colors ON cars_small.color_id = colors.ID " +
            "PREFERENCE LOW cars_small.price AND HIGH colors.name {'rot' == 'pink' >> OTHERS >> 'grau'} ";

            SQLCommon common = new SQLCommon();
            common.SkylineType = SQLCommon.Algorithm.NativeSQL;
            string sqlNative = common.parsePreferenceSQL(strPrefSQL);
            common.SkylineType = SQLCommon.Algorithm.BNL;
            string sqlBNL = common.parsePreferenceSQL(strPrefSQL);

            int amountOfTupelsNative = 0;
            int amountOfTupelsBNL = 0;

            SqlConnection cnnSQL = new SqlConnection(strConnection);
            try
            {
                cnnSQL.Open();

                //Native
                SqlCommand sqlCommand = new SqlCommand(sqlNative, cnnSQL);
                SqlDataReader sqlReader = sqlCommand.ExecuteReader();

                if (sqlReader.HasRows)
                {
                    while (sqlReader.Read())
                    {
                        amountOfTupelsNative++;
                    }
                }
                sqlReader.Close();

                //BNL
                sqlCommand = new SqlCommand(sqlBNL, cnnSQL);
                sqlReader = sqlCommand.ExecuteReader();

                if (sqlReader.HasRows)
                {
                    while (sqlReader.Read())
                    {
                        amountOfTupelsBNL++;
                    }
                }
                cnnSQL.Close();
            }
            catch (Exception ex)
            {
                Assert.Fail("Connection failed:" + ex.Message);
            }

            Assert.AreEqual(amountOfTupelsNative, amountOfTupelsBNL, 0, "Amount of tupels does not match");
        }


        [TestMethod]
        public void TestSKYLINEAmountsOfTupels3()
        {
            string strPrefSQL = "SELECT * FROM cars PREFERENCE LOW cars.price AND LOW cars.mileage AND HIGH cars.horsepower";

            SQLCommon common = new SQLCommon();
            common.SkylineType = SQLCommon.Algorithm.NativeSQL;
            string sqlNative = common.parsePreferenceSQL(strPrefSQL);
            common.SkylineType = SQLCommon.Algorithm.BNL;
            string sqlBNL = common.parsePreferenceSQL(strPrefSQL);

            int amountOfTupelsNative = 0;
            int amountOfTupelsBNL = 0;


            SqlConnection cnnSQL = new SqlConnection(strConnection);
            try
            {
                cnnSQL.Open();

                //Native
                SqlCommand sqlCommand = new SqlCommand(sqlNative, cnnSQL);
                SqlDataReader sqlReader = sqlCommand.ExecuteReader();

                if (sqlReader.HasRows)
                {
                    while (sqlReader.Read())
                    {
                        amountOfTupelsNative++;
                    }
                }
                sqlReader.Close();

                //BNL
                sqlCommand = new SqlCommand(sqlBNL, cnnSQL);
                sqlReader = sqlCommand.ExecuteReader();

                if (sqlReader.HasRows)
                {
                    while (sqlReader.Read())
                    {
                        amountOfTupelsBNL++;
                    }
                }
                cnnSQL.Close();
            }
            catch (Exception ex)
            {
                Assert.Fail("Connection failed:" + ex.Message);
            }

            Assert.AreEqual(amountOfTupelsNative, amountOfTupelsBNL, 0, "Amount of tupels does not match");

        }





        [TestMethod]
        public void TestSKYLINEFavourRot()
        {
            string strPrefSQL = "SELECT cars.id, cars.title, cars.Price, colors.Name FROM cars LEFT OUTER JOIN colors ON cars.color_id = colors.ID PREFERENCE LOW cars.price AND colors.name FAVOUR 'rot'";

            string expected = "SELECT cars.id, cars.title, cars.Price, colors.Name FROM cars LEFT OUTER JOIN colors ON cars.color_id = colors.ID WHERE NOT EXISTS(SELECT cars_INNER.id, cars_INNER.title, cars_INNER.Price, colors_INNER.Name FROM cars cars_INNER LEFT OUTER JOIN colors colors_INNER ON cars_INNER.color_id = colors_INNER.ID WHERE cars_INNER.price <= cars.price AND CASE WHEN colors_INNER.name = 'rot' THEN 1 ELSE 2 END <= CASE WHEN colors.name = 'rot' THEN 1 ELSE 2 END AND ( cars_INNER.price < cars.price OR CASE WHEN colors_INNER.name = 'rot' THEN 1 ELSE 2 END < CASE WHEN colors.name = 'rot' THEN 1 ELSE 2 END) )  ORDER BY price ASC, CASE WHEN colors.name = 'rot' THEN 1 ELSE 2 END ASC";
            SQLCommon common = new SQLCommon();
            string actual = common.parsePreferenceSQL(strPrefSQL);

            // assert

            Assert.AreEqual(expected, actual, true, "SQL not built correctly");
        }

        [TestMethod]
        public void TestSKYLINEDisfavourGruen()
        {
            string strPrefSQL = "SELECT cars.id, cars.title, cars.Price, colors.Name FROM cars LEFT OUTER JOIN colors ON cars.color_id = colors.ID PREFERENCE LOW cars.price AND colors.name DISFAVOUR 'grün'";

            string expected = "SELECT cars.id, cars.title, cars.Price, colors.Name FROM cars LEFT OUTER JOIN colors ON cars.color_id = colors.ID WHERE NOT EXISTS(SELECT cars_INNER.id, cars_INNER.title, cars_INNER.Price, colors_INNER.Name FROM cars cars_INNER LEFT OUTER JOIN colors colors_INNER ON cars_INNER.color_id = colors_INNER.ID WHERE cars_INNER.price <= cars.price AND CASE WHEN colors_INNER.name = 'grün' THEN 1 ELSE 2 END >= CASE WHEN colors.name = 'grün' THEN 1 ELSE 2 END AND ( cars_INNER.price < cars.price OR CASE WHEN colors_INNER.name = 'grün' THEN 1 ELSE 2 END > CASE WHEN colors.name = 'grün' THEN 1 ELSE 2 END) )  ORDER BY price ASC, CASE WHEN colors.name = 'grün' THEN 1 ELSE 2 END DESC";
            SQLCommon common = new SQLCommon();
            string actual = common.parsePreferenceSQL(strPrefSQL);

            // assert

            Assert.AreEqual(expected, actual, true, "SQL not built correctly");
        }



        [TestMethod]
        public void TestSKYLINEWithUnconventialJOIN()
        {
            string strPrefSQL = "SELECT cars.id, cars.title, cars.price, colors.name FROM cars, colors WHERE cars.Color_Id = colors.Id PREFERENCE LOW cars.price AND HIGH colors.name {'grau' >> 'rot'}";

            string expected = "SELECT cars.id, cars.title, cars.price, colors.name FROM cars, colors WHERE cars.Color_Id = colors.Id AND NOT EXISTS(SELECT cars_INNER.id, cars_INNER.title, cars_INNER.price, colors_INNER.name FROM cars cars_INNER, colors colors_INNER WHERE cars_INNER.Color_Id = colors_INNER.Id  AND cars_INNER.price <= cars.price AND (CASE WHEN colors_INNER.name = 'grau' THEN 0 WHEN colors_INNER.name = 'rot' THEN 100 END <= CASE WHEN colors.name = 'grau' THEN 0 WHEN colors.name = 'rot' THEN 100 END OR colors_INNER.name = colors.name) AND ( cars_INNER.price < cars.price OR CASE WHEN colors_INNER.name = 'grau' THEN 0 WHEN colors_INNER.name = 'rot' THEN 100 END < CASE WHEN colors.name = 'grau' THEN 0 WHEN colors.name = 'rot' THEN 100 END) )  ORDER BY price ASC, CASE WHEN colors.name = 'grau' THEN 0 WHEN colors.name = 'rot' THEN 100 END ASC";
            SQLCommon common = new SQLCommon();
            string actual = common.parsePreferenceSQL(strPrefSQL);

            // assert

            Assert.AreEqual(expected, actual, true, "SQL not built correctly");
        }


        [TestMethod]
        public void TestSKYLINEWithWHEREClause()
        {
            string strPrefSQL = "SELECT cars.id, cars.title, cars.price, cars.mileage FROM cars WHERE cars.price > 10000 PREFERENCE LOW cars.price AND Low cars.mileage";

            string expected = "SELECT cars.id, cars.title, cars.price, cars.mileage FROM cars WHERE cars.price > 10000 AND NOT EXISTS(SELECT cars_INNER.id, cars_INNER.title, cars_INNER.price, cars_INNER.mileage FROM cars cars_INNER WHERE cars_INNER.price > 10000  AND cars_INNER.price <= cars.price AND cars_INNER.mileage <= cars.mileage AND ( cars_INNER.price < cars.price OR cars_INNER.mileage < cars.mileage) )  ORDER BY price ASC, mileage ASC";
            SQLCommon common = new SQLCommon();
            string actual = common.parsePreferenceSQL(strPrefSQL);

            // assert

            Assert.AreEqual(expected, actual, true, "SQL not built correctly");
        }




        [TestMethod]
        public void TestSKYLINETOPKeyword()
        {
            string strPrefSQL = "SELECT TOP 5 cars.id, cars.title, cars.Price, colors.Name FROM cars LEFT OUTER JOIN colors ON cars.color_id = colors.ID PREFERENCE LOW cars.price AND HIGH colors.name {'pink' >> {'rot', 'schwarz'} >> 'beige' == 'gelb'}";

            string expected = "SELECT TOP 5 cars.id, cars.title, cars.Price, colors.Name FROM cars LEFT OUTER JOIN colors ON cars.color_id = colors.ID WHERE NOT EXISTS(SELECT cars_INNER.id, cars_INNER.title, cars_INNER.Price, colors_INNER.Name FROM cars cars_INNER LEFT OUTER JOIN colors colors_INNER ON cars_INNER.color_id = colors_INNER.ID WHERE cars_INNER.price <= cars.price AND (CASE WHEN colors_INNER.name = 'pink' THEN 0 WHEN colors_INNER.name IN ('rot','schwarz') THEN 101 WHEN colors_INNER.name = 'beige' THEN 200 WHEN colors_INNER.name = 'gelb' THEN 200 END <= CASE WHEN colors.name = 'pink' THEN 0 WHEN colors.name IN ('rot','schwarz') THEN 100 WHEN colors.name = 'beige' THEN 200 WHEN colors.name = 'gelb' THEN 200 END OR colors_INNER.name = colors.name) AND ( cars_INNER.price < cars.price OR CASE WHEN colors_INNER.name = 'pink' THEN 0 WHEN colors_INNER.name IN ('rot','schwarz') THEN 101 WHEN colors_INNER.name = 'beige' THEN 200 WHEN colors_INNER.name = 'gelb' THEN 200 END < CASE WHEN colors.name = 'pink' THEN 0 WHEN colors.name IN ('rot','schwarz') THEN 100 WHEN colors.name = 'beige' THEN 200 WHEN colors.name = 'gelb' THEN 200 END) )  ORDER BY price ASC, CASE WHEN colors.name = 'pink' THEN 0 WHEN colors.name IN ('rot','schwarz') THEN 100 WHEN colors.name = 'beige' THEN 200 WHEN colors.name = 'gelb' THEN 200 END ASC";
            SQLCommon common = new SQLCommon();
            string actual = common.parsePreferenceSQL(strPrefSQL);

            // assert

            Assert.AreEqual(expected, actual, true, "SQL not built correctly");
        }


        [TestMethod]
        public void TestSKYLINE2DimensionsJoinMultipleAccumulation()
        {
            string strPrefSQL = "SELECT cars.id, cars.title, cars.Price, colors.Name FROM cars LEFT OUTER JOIN colors ON cars.color_id = colors.ID PREFERENCE LOW cars.price AND HIGH colors.name {'pink' >> {'rot', 'schwarz'} >> 'beige' == 'gelb'}";

            string expected = "SELECT cars.id, cars.title, cars.Price, colors.Name FROM cars LEFT OUTER JOIN colors ON cars.color_id = colors.ID WHERE NOT EXISTS(SELECT cars_INNER.id, cars_INNER.title, cars_INNER.Price, colors_INNER.Name FROM cars cars_INNER LEFT OUTER JOIN colors colors_INNER ON cars_INNER.color_id = colors_INNER.ID WHERE cars_INNER.price <= cars.price AND (CASE WHEN colors_INNER.name = 'pink' THEN 0 WHEN colors_INNER.name IN ('rot','schwarz') THEN 101 WHEN colors_INNER.name = 'beige' THEN 200 WHEN colors_INNER.name = 'gelb' THEN 200 END <= CASE WHEN colors.name = 'pink' THEN 0 WHEN colors.name IN ('rot','schwarz') THEN 100 WHEN colors.name = 'beige' THEN 200 WHEN colors.name = 'gelb' THEN 200 END OR colors_INNER.name = colors.name) AND ( cars_INNER.price < cars.price OR CASE WHEN colors_INNER.name = 'pink' THEN 0 WHEN colors_INNER.name IN ('rot','schwarz') THEN 101 WHEN colors_INNER.name = 'beige' THEN 200 WHEN colors_INNER.name = 'gelb' THEN 200 END < CASE WHEN colors.name = 'pink' THEN 0 WHEN colors.name IN ('rot','schwarz') THEN 100 WHEN colors.name = 'beige' THEN 200 WHEN colors.name = 'gelb' THEN 200 END) )  ORDER BY price ASC, CASE WHEN colors.name = 'pink' THEN 0 WHEN colors.name IN ('rot','schwarz') THEN 100 WHEN colors.name = 'beige' THEN 200 WHEN colors.name = 'gelb' THEN 200 END ASC";
            SQLCommon common = new SQLCommon();
            string actual = common.parsePreferenceSQL(strPrefSQL);

            // assert

            Assert.AreEqual(expected, actual, true, "SQL not built correctly");
        }







        [TestMethod]
        public void TestSKYLINE2DimensionsJoinOthersAccumulation()
        {
            string strPrefSQL = "SELECT cars.id, cars.title, cars.Price, colors.Name FROM cars LEFT OUTER JOIN colors ON cars.color_id = colors.ID PREFERENCE LOW cars.price AND HIGH colors.name {'türkis' >> 'gelb' >> OTHERS}";

            string expected = "SELECT cars.id, cars.title, cars.Price, colors.Name FROM cars LEFT OUTER JOIN colors ON cars.color_id = colors.ID WHERE NOT EXISTS(SELECT cars_INNER.id, cars_INNER.title, cars_INNER.Price, colors_INNER.Name FROM cars cars_INNER LEFT OUTER JOIN colors colors_INNER ON cars_INNER.color_id = colors_INNER.ID WHERE cars_INNER.price <= cars.price AND (CASE WHEN colors_INNER.name = 'türkis' THEN 0 WHEN colors_INNER.name = 'gelb' THEN 100 ELSE 201 END <= CASE WHEN colors.name = 'türkis' THEN 0 WHEN colors.name = 'gelb' THEN 100 ELSE 200 END OR colors_INNER.name = colors.name) AND ( cars_INNER.price < cars.price OR CASE WHEN colors_INNER.name = 'türkis' THEN 0 WHEN colors_INNER.name = 'gelb' THEN 100 ELSE 201 END < CASE WHEN colors.name = 'türkis' THEN 0 WHEN colors.name = 'gelb' THEN 100 ELSE 200 END) )  ORDER BY price ASC, CASE WHEN colors.name = 'türkis' THEN 0 WHEN colors.name = 'gelb' THEN 100 ELSE 200 END ASC";
            SQLCommon common = new SQLCommon();
            string actual = common.parsePreferenceSQL(strPrefSQL);

            // assert

            Assert.AreEqual(expected, actual, true, "SQL not built correctly");
        }


        [TestMethod]
        public void TestSKYLINE2DimensionsJoinNoOthers()
        {
            string strPrefSQL = "SELECT cars.id, cars.title, cars.Price, colors.Name FROM cars LEFT OUTER JOIN colors ON cars.color_id = colors.ID PREFERENCE LOW cars.price AND HIGH colors.name {'pink' >> 'rot' == 'schwarz' >> 'beige' == 'gelb'}";

            string expected = "SELECT cars.id, cars.title, cars.Price, colors.Name FROM cars LEFT OUTER JOIN colors ON cars.color_id = colors.ID WHERE NOT EXISTS(SELECT cars_INNER.id, cars_INNER.title, cars_INNER.Price, colors_INNER.Name FROM cars cars_INNER LEFT OUTER JOIN colors colors_INNER ON cars_INNER.color_id = colors_INNER.ID WHERE cars_INNER.price <= cars.price AND (CASE WHEN colors_INNER.name = 'pink' THEN 0 WHEN colors_INNER.name = 'rot' THEN 100 WHEN colors_INNER.name = 'schwarz' THEN 100 WHEN colors_INNER.name = 'beige' THEN 200 WHEN colors_INNER.name = 'gelb' THEN 200 END <= CASE WHEN colors.name = 'pink' THEN 0 WHEN colors.name = 'rot' THEN 100 WHEN colors.name = 'schwarz' THEN 100 WHEN colors.name = 'beige' THEN 200 WHEN colors.name = 'gelb' THEN 200 END OR colors_INNER.name = colors.name) AND ( cars_INNER.price < cars.price OR CASE WHEN colors_INNER.name = 'pink' THEN 0 WHEN colors_INNER.name = 'rot' THEN 100 WHEN colors_INNER.name = 'schwarz' THEN 100 WHEN colors_INNER.name = 'beige' THEN 200 WHEN colors_INNER.name = 'gelb' THEN 200 END < CASE WHEN colors.name = 'pink' THEN 0 WHEN colors.name = 'rot' THEN 100 WHEN colors.name = 'schwarz' THEN 100 WHEN colors.name = 'beige' THEN 200 WHEN colors.name = 'gelb' THEN 200 END) )  ORDER BY price ASC, CASE WHEN colors.name = 'pink' THEN 0 WHEN colors.name = 'rot' THEN 100 WHEN colors.name = 'schwarz' THEN 100 WHEN colors.name = 'beige' THEN 200 WHEN colors.name = 'gelb' THEN 200 END ASC";
            SQLCommon common = new SQLCommon();
            string actual = common.parsePreferenceSQL(strPrefSQL);

            // assert

            Assert.AreEqual(expected, actual, true, "SQL not built correctly");
        }



        [TestMethod]
        public void TestSKYLINE2DimensionsNoJoin()
        {
            string strPrefSQL = "SELECT cars.id, cars.price, cars.title FROM cars PREFERENCE HIGH cars.title {'MERCEDES-BENZ SL 600' >> OTHERSEQUAL} AND LOW cars.price";

            string expected = "SELECT cars.id, cars.price, cars.title FROM cars WHERE NOT EXISTS(SELECT cars_INNER.id, cars_INNER.price, cars_INNER.title FROM cars cars_INNER WHERE (CASE WHEN cars_INNER.title = 'MERCEDES-BENZ SL 600' THEN 0 ELSE 100 END <= CASE WHEN cars.title = 'MERCEDES-BENZ SL 600' THEN 0 ELSE 100 END OR cars_INNER.title = cars.title) AND cars_INNER.price <= cars.price AND ( CASE WHEN cars_INNER.title = 'MERCEDES-BENZ SL 600' THEN 0 ELSE 100 END < CASE WHEN cars.title = 'MERCEDES-BENZ SL 600' THEN 0 ELSE 100 END OR cars_INNER.price < cars.price) )  ORDER BY CASE WHEN cars.title = 'MERCEDES-BENZ SL 600' THEN 0 ELSE 100 END ASC, price ASC";
            SQLCommon common = new SQLCommon();
            string actual = common.parsePreferenceSQL(strPrefSQL);

            // assert

            Assert.AreEqual(expected, actual, true, "SQL not built correctly");
        }



        [TestMethod]
        public void TestSKYLINE2DimensionsWithJoin()
        {
            string strPrefSQL = "SELECT cars.id, cars.price, cars.title, colors.name FROM cars LEFT OUTER JOIN colors ON cars.color_id = colors.ID PREFERENCE HIGH colors.name {'rot' >> OTHERSEQUAL} AND LOW cars.price";

            string expected = "SELECT cars.id, cars.price, cars.title, colors.name FROM cars LEFT OUTER JOIN colors ON cars.color_id = colors.ID WHERE NOT EXISTS(SELECT cars_INNER.id, cars_INNER.price, cars_INNER.title, colors_INNER.name FROM cars cars_INNER LEFT OUTER JOIN colors colors_INNER ON cars_INNER.color_id = colors_INNER.ID WHERE (CASE WHEN colors_INNER.name = 'rot' THEN 0 ELSE 100 END <= CASE WHEN colors.name = 'rot' THEN 0 ELSE 100 END OR colors_INNER.name = colors.name) AND cars_INNER.price <= cars.price AND ( CASE WHEN colors_INNER.name = 'rot' THEN 0 ELSE 100 END < CASE WHEN colors.name = 'rot' THEN 0 ELSE 100 END OR cars_INNER.price < cars.price) )  ORDER BY CASE WHEN colors.name = 'rot' THEN 0 ELSE 100 END ASC, price ASC";

            SQLCommon common = new SQLCommon();
            string actual = common.parsePreferenceSQL(strPrefSQL);

            // assert

            Assert.AreEqual(expected, actual, true, "SQL not built correctly");
        }


        [TestMethod]
        public void TestSKYLINE2Dimensions()
        {
            string strPrefSQL = "SELECT * FROM cars PREFERENCE LOW cars.price AND LOW cars.mileage";

            string expected = "SELECT * FROM cars WHERE NOT EXISTS(SELECT * FROM cars cars_INNER WHERE cars_INNER.price <= cars.price AND cars_INNER.mileage <= cars.mileage AND ( cars_INNER.price < cars.price OR cars_INNER.mileage < cars.mileage) )  ORDER BY price ASC, mileage ASC";
            SQLCommon common = new SQLCommon();
            string actual = common.parsePreferenceSQL(strPrefSQL);

            // assert

            Assert.AreEqual(expected, actual, true, "SQL not built correctly");



        }


        [TestMethod]
        public void TestSKYLINE2DimensionswithALIAS()
        {
            string strPrefSQL = "SELECT * FROM cars t1 PREFERENCE LOW t1.price AND LOW t1.mileage";

            string expected = "SELECT * FROM cars t1 WHERE NOT EXISTS(SELECT * FROM cars t1_INNER WHERE t1_INNER.price <= t1.price AND t1_INNER.mileage <= t1.mileage AND ( t1_INNER.price < t1.price OR t1_INNER.mileage < t1.mileage) )  ORDER BY price ASC, mileage ASC";
            SQLCommon common = new SQLCommon();
            string actual = common.parsePreferenceSQL(strPrefSQL);

            // assert

            Assert.AreEqual(expected, actual, true, "SQL not built correctly");



        }


        [TestMethod]
        public void TestSKYLINE3Dimensions()
        {
            string strPrefSQL = "SELECT * FROM cars PREFERENCE LOW cars.price AND LOW cars.mileage AND HIGH cars.horsepower";

            string expected = "SELECT * FROM cars WHERE NOT EXISTS(SELECT * FROM cars cars_INNER WHERE cars_INNER.price <= cars.price AND cars_INNER.mileage <= cars.mileage AND cars_INNER.horsepower >= cars.horsepower AND ( cars_INNER.price < cars.price OR cars_INNER.mileage < cars.mileage OR cars_INNER.horsepower > cars.horsepower) )  ORDER BY price ASC, mileage ASC, horsepower DESC";
            SQLCommon common = new SQLCommon();
            string actual = common.parsePreferenceSQL(strPrefSQL);

            // assert

            Assert.AreEqual(expected, actual, true, "SQL not built correctly");



        }


        [TestMethod]
        public void TestSKYLINE6Dimensions()
        {
            string strPrefSQL = "SELECT * FROM cars PREFERENCE LOW cars.price AND LOW cars.mileage AND HIGH cars.horsepower AND HIGH cars.enginesize AND HIGHDATE cars.registration AND LOW cars.consumption";

            string expected = "SELECT * FROM cars WHERE NOT EXISTS(SELECT * FROM cars cars_INNER WHERE cars_INNER.price <= cars.price AND cars_INNER.mileage <= cars.mileage AND cars_INNER.horsepower >= cars.horsepower AND cars_INNER.enginesize >= cars.enginesize AND DATEDIFF(minute, '1900-01-01', cars_INNER.registration) >= DATEDIFF(minute, '1900-01-01', cars.registration) AND cars_INNER.consumption <= cars.consumption AND ( cars_INNER.price < cars.price OR cars_INNER.mileage < cars.mileage OR cars_INNER.horsepower > cars.horsepower OR cars_INNER.enginesize > cars.enginesize OR DATEDIFF(minute, '1900-01-01', cars_INNER.registration) > DATEDIFF(minute, '1900-01-01', cars.registration) OR cars_INNER.consumption < cars.consumption) )  ORDER BY price ASC, mileage ASC, horsepower DESC, enginesize DESC, registration DESC, consumption ASC";
            SQLCommon common = new SQLCommon();
            string actual = common.parsePreferenceSQL(strPrefSQL);

            // assert

            Assert.AreEqual(expected, actual, true, "SQL not built correctly");



        }



        [TestMethod]
        public void TestSKYLINEAROUND()
        {
            string strPrefSQL = "SELECT * FROM cars PREFERENCE cars.price AROUND 15000 AND LOW cars.mileage";

            string expected = "SELECT * FROM cars WHERE NOT EXISTS(SELECT * FROM cars cars_INNER WHERE ABS(cars_INNER.price - 15000) <= ABS(cars.price - 15000) AND cars_INNER.mileage <= cars.mileage AND ( ABS(cars_INNER.price - 15000) < ABS(cars.price - 15000) OR cars_INNER.mileage < cars.mileage) )  ORDER BY ABS(cars.price - 15000) ASC, mileage ASC";
            SQLCommon common = new SQLCommon();
            string actual = common.parsePreferenceSQL(strPrefSQL);

            Assert.AreEqual(expected, actual, true, "SQL not built correctly");

        }

    }
}
