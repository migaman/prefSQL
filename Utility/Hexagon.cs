using System;
using System.Data;
using System.Data.SqlClient;
using System.Data.SqlTypes;
using Microsoft.SqlServer.Server;
using System.Collections;
using System.Collections.Generic;


namespace Utility
{
    class Hexagon
    {
        //Only this parameters are different beteen SQL CLR function and Utility class
        private const string connectionstring = "Data Source=localhost;Initial Catalog=eCommerce;Integrated Security=True";
        private const int MaxSize = 4000;


        private static ArrayList[] btg;
        private static int[] next;
        private static int[] prev;
        private static int[] level;
        private static int[] weight;


        [Microsoft.SqlServer.Server.SqlProcedure]
        public static void SP_SkylineHexagon(SqlString strQuery, SqlString strOperators)
        {

            String strSQL = strQuery.ToString();
            ArrayList resultCollection = new ArrayList();
            ArrayList resultstringCollection = new ArrayList();
            string[] operators = strOperators.ToString().Split(';');
            int amountOfPreferences = operators.Length;
            construction(amountOfPreferences);
            

            SqlConnection connection = new SqlConnection(connectionstring);
            try
            {
                //Some checks
                if (strSQL.ToString().Length == MaxSize)
                {
                    throw new Exception("Query is too long. Maximum size is " + MaxSize);
                }
                connection.Open();

                strSQL = "SELECT " +
                    //"DENSE_RANK() OVER (ORDER BY CASE WHEN t2.Name = 'schwarz' THEN 1 ELSE 2 END) AS Skyline1, " +
                    "DENSE_RANK() OVER (ORDER BY t1.mileage) AS Skyline1, " +
                    "DENSE_RANK() OVER (ORDER BY t1.price) AS Skyline2 " +
                    "FROM Cars_small t1 LEFT OUTER JOIN colors t2 ON t1.color_id = t2.ID " +
                    "WHERE t1.price < 2200";
                //strSQL = "SELECT DENSE_RANK() OVER (ORDER BY CASE WHEN t2.Name = 'schwarz' THEN 1 ELSE 2 END) AS Skyline1, DENSE_RANK() OVER (ORDER BY t1.price) AS Skyline2 FROM Cars_small t1 LEFT OUTER JOIN colors t2 ON t1.color_id = t2.ID";

                SqlDataAdapter dap = new SqlDataAdapter(strSQL.ToString(), connection);
                DataTable dt = new DataTable();
                dap.Fill(dt);


                // Build our record schema 
                //List<SqlMetaData> outputColumns = buildRecordSchema(dt, operators);

                //SqlDataRecord record = new SqlDataRecord(outputColumns.ToArray());
                DataTableReader sqlReader = dt.CreateDataReader();


                //Read all records only once. (SqlDataReader works forward only!!)
                while (sqlReader.Read())
                {
                    add(sqlReader);
                }
                sqlReader.Close();

                findBMO(amountOfPreferences);
                //Now read next list
                int iLoop = 0;
                while(iLoop != -1) {
                    int iNode = next[iLoop];
                    if (iNode == -1)
                        break;
                    if(btg[iNode] != null) {
                        foreach (long[] test in btg[iNode])
                    {
                        resultCollection.Add(test);
                    }
                    }
                    
                    iLoop = iNode;
                }
                System.Diagnostics.Debug.WriteLine(resultCollection.Count);

                

            }
            catch (Exception ex)
            {
                //Pack Errormessage in a SQL and return the result
                string strError = "SELECT 'Fehler in SP_SkylineBNL: ";
                strError += ex.Message.Replace("'", "''");
                strError += "'";

                //TODO: only for real Stored Procedure
                SqlContext.Pipe.Send(strError);

            }
            finally
            {
                if (connection != null)
                    connection.Close();
            }

        }

        private static void remove(int id, int index) {
            //check if the node has already been removed
            if (prev[id] == -1)
            {
                return;
            }
            if (next[prev[id]] != id) {
                return;
            }
            else
            {  
                //remove the node from next/prev relation
                next[prev[id]] = next[id];
                if (next[id] != -1)
                {
                    prev[next[id]] = prev[id];
                }
                //else
                /*{
                    prev[id] = -1;
                }*/
                //prev[id] = prev[id];

                next[id] = -1;
                prev[id] = -1;

                //remove tuples in node
                btg[id] = null;

            }
          
            //remove followers
            for(int i = 1; i <= index; i++) {
                // follow the edge for preference i (only if not already on last level)
                if (id+i <= level.GetUpperBound(0) &&  level[id + i] == level[id] + 1)
                {
                    remove(id + i, i);
                }
            }

        }

        //Find BestMatchesOnly
        private static void findBMO(int amountPreferences)
        {
            int last = 0;

            // special case: top node is not empty
            if(btg[0] != null)
            {
                //Top node contains a tuple --> This is the perfect tuple. Set next to -1,
                next[0] = -1;
                
            }
            else
            {
                //follow the breadth-first walk
                int cur = 1;
                //while (cur != -1)
                while (cur != -1 && next[cur] != -1) //<= btg.GetUpperBound(0)
                {
                    if(btg[cur] != null) 
                    {
                        // non-empty node belonging to BMO set
                        int nextCur = next[cur];
                        last = cur;
                        // remove all nodes dominated by current
                        for (int i = 0; i < amountPreferences; i++)
                        {
                            // check if there is an edge for Pi
                            if (cur + weight[i] <= level.GetUpperBound(0))
                            {
                                if (level[cur + weight[i]] == level[cur] + 1)
                                {
                                    remove(cur + weight[i], i);
                                }
                            }
                            
                            
                        }
                        cur = nextCur;
                        
                    }
                    else 
                    {
                        // node is empty: remove from next/prev
                        int nextCur = next[cur];
                        
                        next[last] = next[cur];
                        prev[next[cur]] = last;
                        next[cur] = -1; //gibt es nicht mehr dieses node
                        prev[cur] = -1;
                        /*next[last] = cur;
                        if (next[cur] != -1)
                        {
                            prev[next[cur]] = last;
                        }*/

                        cur = nextCur; //Damit Breadt-First Walk
                    }

                    //Goto next node
                    //cur++;
                } 

            }
            
           

        }
        private static void add(DataTableReader sqlReader) //add tuple
        {
            //create int array from sqlReader
            long[] tuple = new long[sqlReader.FieldCount];
            for (int iCol = 0; iCol < sqlReader.FieldCount; iCol++)
            {
                tuple[iCol] = sqlReader.GetInt64(iCol);
            }



            //1: procedure add(tuple)
            // compute the node ID for the tuple
            long id = 0;
            int m = 2;
            for(int i = 0; i < m; i++)
            {
                //id = id + levelPi(tuple) * weight(i);
                id = id + tuple[i] * weight[i];
            }
            
            // add tuple to its node
            if (btg[id] == null)
            {
                btg[id] = new ArrayList();     
            }
            btg[id].Add(tuple); 
            
            
        }


        private static void construction(int amountOfPreferences)
        {
            SqlConnection connection = new SqlConnection(connectionstring);
            try
            {
                connection.Open();
                String strQuery = "SELECT MAX(Level_Price), MAX(Level_Colour) FROM ( " +
	                                    "SELECT " +
		                                "      t1.id " +
		                                "    , price " +
		                                "    , DENSE_RANK() OVER (ORDER BY price) AS Level_Price " +
		                                "    , Colors.Name AS Colour " +
		                                //"    , DENSE_RANK() OVER (ORDER BY CASE WHEN Colors.Name = 'schwarz' THEN 1 ELSE 2 END) AS Level_Colour " +
                                        "    , DENSE_RANK() OVER (ORDER BY mileage) AS Level_Colour " +
	                                    "FROM Cars_small t1 " +
	                                    "LEFT OUTER JOIN Colors on t1.Color_Id = Colors.Id " +
                                        "WHERE t1.price < 2200 " +
                                        ") " +
	                                    "MyQuery";
                SqlDataAdapter dap = new SqlDataAdapter(strQuery, connection);
                DataTable dt = new DataTable();
                dap.Fill(dt);



                int[] maxPreferenceLevel = new int[amountOfPreferences];
                maxPreferenceLevel[0] = int.Parse(dt.Rows[0][0].ToString());
                maxPreferenceLevel[1] = int.Parse(dt.Rows[0][1].ToString());

                //calculate edge weights
                weight = new int[amountOfPreferences];
                weight[amountOfPreferences - 1] = 1;

                for (int i = amountOfPreferences - 2; i >= 0; i--)
                {
                    weight[i] = weight[i + 1] * (maxPreferenceLevel[i] + 1);
                }

                
                // calculate the BTG size
                int sizeNodes = (maxPreferenceLevel[0] + 1) * (maxPreferenceLevel[1] + 1);

                //ArrayList listOfTuples 
                btg = new ArrayList[sizeNodes];
                next= new int[sizeNodes]; 
                prev = new int[sizeNodes]; 
                level = new int[sizeNodes];

                int workSize = maxPreferenceLevel[0] + maxPreferenceLevel[1] + 1;
                // arrays needed for init computation
                
                // stores highest ID found for each level by now
                int[] work = new int[workSize]; //int[max(P) + 1]
                
                // stores first ID found for each level by now
                int[] first = new int[workSize]; //int[max(P) + 1]
                
                // initialize the arrays
                next[0] = 1;

                //loop over the node IDs
                for (int id = 1; id <= sizeNodes - 1; id++)
                {
                    // compute level of the node (with help of weights)
                    int curLvl = 0;
                    int tmpID = id; //wrong code in paper --> use of id inside de id-loop
                    for (int i = 0; i <= amountOfPreferences - 1; i++)
                    {
                        double dblLevel = (double)tmpID / weight[i];
                        curLvl = curLvl + (int)Math.Floor(dblLevel);
                        tmpID = tmpID - ((int)Math.Floor((dblLevel)) * weight[i]);
                    }

                    if (first[curLvl] == 0) {
                        first[curLvl] = id;
                    }
                    // set next node in the level
                    //the current node is n’s next node and n is the current node’s previous node
                    next[work[curLvl]] = id;
                    prev[id] = work[curLvl];
                    work[curLvl] = id; //wrong code in paper
                    
                    //For each ID, we determine to which level the corresponding node belongs
                    level[id] = curLvl;
                }

                // init next relation for last nodes in the levels
                for (int curLvl = 0; curLvl < workSize - 1; curLvl++)
                {
                    next[work[curLvl]] = first[curLvl + 1];

                    //
                    prev[first[curLvl+1]] = work[curLvl];
                }
                /*for (int curLvl = 1; curLvl < workSize; curLvl++)
                {
                    prev[first[curLvl]] = work[curLvl - 1];
                }*/
                
                //set next node of bottom node
                next[sizeNodes - 1] = -1;
                //set previous node of top node
                prev[0] = -1;



            }
            catch(Exception e)
            {
                System.Diagnostics.Debug.WriteLine(e.Message);
            }
        }



        private static List<SqlMetaData> buildRecordSchema(DataTable dt, string[] operators)
        {
            List<SqlMetaData> outputColumns = new List<SqlMetaData>(dt.Columns.Count);
            int iCol = 0;
            foreach (DataColumn col in dt.Columns)
            {
                //Only the real columns (skyline columns are not output fields)
                if (iCol > operators.GetUpperBound(0))
                {
                    SqlMetaData OutputColumn;
                    if (col.DataType.Equals(typeof(Int32)) || col.DataType.Equals(typeof(DateTime)))
                    {
                        OutputColumn = new SqlMetaData(col.ColumnName, Utility.TypeConverter.ToSqlDbType(col.DataType));
                    }
                    else
                    {
                        OutputColumn = new SqlMetaData(col.ColumnName, Utility.TypeConverter.ToSqlDbType(col.DataType), col.MaxLength);
                    }
                    outputColumns.Add(OutputColumn);
                }
                iCol++;
            }
            return outputColumns;
        }



    }




}
