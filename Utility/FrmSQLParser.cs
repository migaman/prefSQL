using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Drawing;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Windows.Forms;
using prefSQL.SQLParser;
using System.Diagnostics;

namespace Utility
{
    public partial class FrmSQLParser : Form
    {
        private const string cnnStringLocalhost = "Data Source=localhost;Initial Catalog=eCommerce;Integrated Security=True";
        private const string driver = "System.Data.SqlClient";

        public FrmSQLParser()
        {
            InitializeComponent();
        }

        private void FrmSQLParser_Load(object sender, EventArgs e)
        {
            this.optSQL.Checked = true;
            string strSQL = "SELECT t1.id, t1.title, t1.price, t1.mileage, t1.enginesize FROM cars t1 " +
                "LEFT OUTER JOIN colors ON t1.color_id = colors.ID SKYLINE OF t1.price LOW, t1.mileage LOW";
            this.txtPrefSQL.Text = strSQL;


        }


        private void btnExecute_Click(object sender, EventArgs e)
        {
            Stopwatch sw = new Stopwatch();
            sw.Start();

            this.btnExecute.Enabled = false;
            
            SQLCommon parser = new SQLCommon();
            if(this.optSQL.Checked == true)
            {
                parser.SkylineType = SQLCommon.Algorithm.NativeSQL;
            }
            else if (this.optBNL.Checked == true)
            {
                parser.SkylineType = SQLCommon.Algorithm.BNLSort;
            }
            else if (this.optHexagon.Checked == true)
            {
                parser.SkylineType = SQLCommon.Algorithm.Hexagon;
            }
            

            DataTable dt = parser.parseAndExecutePrefSQL(cnnStringLocalhost, driver, this.txtPrefSQL.Text);
            System.Diagnostics.Debug.WriteLine(dt.Rows.Count);


            BindingSource SBind = new BindingSource();
            SBind.DataSource = dt;
            
            gridSkyline.AutoGenerateColumns = true;
            gridSkyline.DataSource = dt;

            gridSkyline.DataSource = SBind;
            gridSkyline.Refresh();


            this.btnExecute.Enabled = true;



            this.txtTime.Text = sw.Elapsed.ToString();          
        }

    }
}
