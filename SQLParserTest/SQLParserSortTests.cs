using Microsoft.VisualStudio.TestTools.UnitTesting;
using prefSQL.SQLParser;

namespace prefSQL.SQLParserTest
{
    [TestClass]
    public class SQLParserSortTests
    {
        [TestMethod]
        public void TestParserOrderingAsIs()
        {
            string strPrefSQL = "SELECT * FROM cars SKYLINE OF cars.price LOW, cars.mileage LOW ORDER BY cars.title";

            string expected = "SELECT * FROM cars WHERE NOT EXISTS(SELECT * FROM cars cars_INNER WHERE cars_INNER.price <= cars.price AND cars_INNER.mileage <= cars.mileage AND ( cars_INNER.price < cars.price OR cars_INNER.mileage < cars.mileage) ) ORDER BY cars.title";
            SQLCommon common = new SQLCommon();
            string actual = common.ParsePreferenceSQL(strPrefSQL);

            // assert

            Assert.AreEqual(expected, actual, true, "SQL not built correctly");
        }


        [TestMethod]
        public void TestParserOrderingAsIsWithCategory()
        {
            string strPrefSQL = "SELECT t1.id, t1.title, t2.name, bodies.name FROM cars_small t1 " +
                "LEFT OUTER JOIN colors t2 ON t1.color_id = t2.ID " +
                "LEFT OUTER JOIN bodies ON t1.body_id = bodies.ID " +
                "SKYLINE OF t1.price LOW, t1.mileage LOW " +  
                "ORDER BY bodies.name ('minivan' >> OTHERS EQUAL), t2.name ('white' >> OTHERS EQUAL) ";

            string expected = "SELECT t1.id, t1.title, t2.name, bodies.name FROM cars_small t1 LEFT OUTER JOIN colors t2 ON t1.color_id = t2.ID LEFT OUTER JOIN bodies ON t1.body_id = bodies.ID WHERE NOT EXISTS(SELECT t1_INNER.id, t1_INNER.title, t2_INNER.name, bodies_INNER.name FROM cars_small t1_INNER LEFT OUTER JOIN colors t2_INNER ON t1_INNER.color_id = t2_INNER.ID LEFT OUTER JOIN bodies bodies_INNER ON t1_INNER.body_id = bodies_INNER.ID WHERE t1_INNER.price <= t1.price AND t1_INNER.mileage <= t1.mileage AND ( t1_INNER.price < t1.price OR t1_INNER.mileage < t1.mileage) ) ORDER BY CASE WHEN bodies.name = 'minivan' THEN 0 ELSE 100 END ASC, CASE WHEN t2.name = 'white' THEN 0 ELSE 100 END ASC ";
            SQLCommon common = new SQLCommon();
            string actual = common.ParsePreferenceSQL(strPrefSQL);

            // assert

            Assert.AreEqual(expected, actual, true, "SQL not built correctly");
        }


        

        [TestMethod]
        public void TestParserOrderingRankingBestOf()
        {
            string strPrefSQL = "SELECT * FROM cars SKYLINE OF cars.price LOW, cars.mileage LOW, cars.horsepower HIGH ORDER BY BEST_RANK()";

            string expected = "SELECT * FROM cars WHERE NOT EXISTS(SELECT * FROM cars cars_INNER WHERE cars_INNER.price <= cars.price AND cars_INNER.mileage <= cars.mileage AND cars_INNER.horsepower * -1 <= cars.horsepower * -1 AND ( cars_INNER.price < cars.price OR cars_INNER.mileage < cars.mileage OR cars_INNER.horsepower * -1 < cars.horsepower * -1) )  ORDER BY CASE WHEN ROW_NUMBER() over (ORDER BY cars.price) <=ROW_NUMBER() over (ORDER BY cars.mileage) AND ROW_NUMBER() over (ORDER BY cars.price) <=ROW_NUMBER() over (ORDER BY cars.horsepower * -1) THEN ROW_NUMBER() over (ORDER BY cars.price) WHEN ROW_NUMBER() over (ORDER BY cars.mileage) <=ROW_NUMBER() over (ORDER BY cars.horsepower * -1) THEN ROW_NUMBER() over (ORDER BY cars.mileage)  ELSE ROW_NUMBER() over (ORDER BY cars.horsepower * -1) END";
            SQLCommon common = new SQLCommon();
            string actual = common.ParsePreferenceSQL(strPrefSQL);

            // assert

            Assert.AreEqual(expected, actual, true, "SQL not built correctly");



        }


        [TestMethod]
        public void TestParserOrderingRankingSum()
        {
            string strPrefSQL = "SELECT * FROM cars SKYLINE OF cars.price LOW, cars.mileage LOW, cars.horsepower HIGH ORDER BY SUM_RANK()";

            string expected = "SELECT * FROM cars WHERE NOT EXISTS(SELECT * FROM cars cars_INNER WHERE cars_INNER.price <= cars.price AND cars_INNER.mileage <= cars.mileage AND cars_INNER.horsepower * -1 <= cars.horsepower * -1 AND ( cars_INNER.price < cars.price OR cars_INNER.mileage < cars.mileage OR cars_INNER.horsepower * -1 < cars.horsepower * -1) )  ORDER BY DENSE_RANK() over (ORDER BY cars.price) + DENSE_RANK() over (ORDER BY cars.mileage) + DENSE_RANK() over (ORDER BY cars.horsepower * -1)";
            SQLCommon common = new SQLCommon();
            string actual = common.ParsePreferenceSQL(strPrefSQL);

            // assert

            Assert.AreEqual(expected, actual, true, "SQL not built correctly");



        }
    }
}
