using System.ComponentModel;
using System.Windows.Forms;

namespace Utility
{
    partial class FrmSQLParser
    {
        /// <summary>
        /// Required designer variable.
        /// </summary>
        private IContainer components = null;

        /// <summary>
        /// Clean up any resources being used.
        /// </summary>
        /// <param name="disposing">true if managed resources should be disposed; otherwise, false.</param>
        protected override void Dispose(bool disposing)
        {
            if (disposing && (components != null))
            {
                components.Dispose();
            }
            base.Dispose(disposing);
        }

        #region Windows Form Designer generated code

        /// <summary>
        /// Required method for Designer support - do not modify
        /// the contents of this method with the code editor.
        /// </summary>
        private void InitializeComponent()
        {
            this.groupBox1 = new System.Windows.Forms.GroupBox();
            this.optHexagon = new System.Windows.Forms.RadioButton();
            this.optBNL = new System.Windows.Forms.RadioButton();
            this.optSQL = new System.Windows.Forms.RadioButton();
            this.chkShowSkyline = new System.Windows.Forms.CheckBox();
            this.btnExecute = new System.Windows.Forms.Button();
            this.txtPrefSQL = new System.Windows.Forms.TextBox();
            this.gridSkyline = new System.Windows.Forms.DataGridView();
            this.label1 = new System.Windows.Forms.Label();
            this.txtTimeAlgo = new System.Windows.Forms.TextBox();
            this.optDQ = new System.Windows.Forms.RadioButton();
            this.txtTime = new System.Windows.Forms.TextBox();
            this.label2 = new System.Windows.Forms.Label();
            this.txtRecords = new System.Windows.Forms.TextBox();
            this.label3 = new System.Windows.Forms.Label();
            this.groupBox1.SuspendLayout();
            ((System.ComponentModel.ISupportInitialize)(this.gridSkyline)).BeginInit();
            this.SuspendLayout();
            // 
            // groupBox1
            // 
            this.groupBox1.Controls.Add(this.optDQ);
            this.groupBox1.Controls.Add(this.optHexagon);
            this.groupBox1.Controls.Add(this.optBNL);
            this.groupBox1.Controls.Add(this.optSQL);
            this.groupBox1.Location = new System.Drawing.Point(12, 12);
            this.groupBox1.Name = "groupBox1";
            this.groupBox1.Size = new System.Drawing.Size(342, 180);
            this.groupBox1.TabIndex = 1;
            this.groupBox1.TabStop = false;
            this.groupBox1.Text = "Skyline Algorithm";
            // 
            // optHexagon
            // 
            this.optHexagon.AutoSize = true;
            this.optHexagon.Location = new System.Drawing.Point(24, 101);
            this.optHexagon.Name = "optHexagon";
            this.optHexagon.Size = new System.Drawing.Size(68, 17);
            this.optHexagon.TabIndex = 3;
            this.optHexagon.TabStop = true;
            this.optHexagon.Text = "Hexagon";
            this.optHexagon.UseVisualStyleBackColor = true;
            // 
            // optBNL
            // 
            this.optBNL.AutoSize = true;
            this.optBNL.Location = new System.Drawing.Point(24, 68);
            this.optBNL.Name = "optBNL";
            this.optBNL.Size = new System.Drawing.Size(46, 17);
            this.optBNL.TabIndex = 2;
            this.optBNL.TabStop = true;
            this.optBNL.Text = "BNL";
            this.optBNL.UseVisualStyleBackColor = true;
            // 
            // optSQL
            // 
            this.optSQL.AutoSize = true;
            this.optSQL.Location = new System.Drawing.Point(24, 36);
            this.optSQL.Name = "optSQL";
            this.optSQL.Size = new System.Drawing.Size(46, 17);
            this.optSQL.TabIndex = 1;
            this.optSQL.TabStop = true;
            this.optSQL.Text = "SQL";
            this.optSQL.UseVisualStyleBackColor = true;
            // 
            // chkShowSkyline
            // 
            this.chkShowSkyline.AutoSize = true;
            this.chkShowSkyline.Location = new System.Drawing.Point(36, 198);
            this.chkShowSkyline.Name = "chkShowSkyline";
            this.chkShowSkyline.Size = new System.Drawing.Size(137, 17);
            this.chkShowSkyline.TabIndex = 3;
            this.chkShowSkyline.Text = "Show Skyline Attributes";
            this.chkShowSkyline.UseVisualStyleBackColor = true;
            // 
            // btnExecute
            // 
            this.btnExecute.Location = new System.Drawing.Point(12, 268);
            this.btnExecute.Name = "btnExecute";
            this.btnExecute.Size = new System.Drawing.Size(129, 28);
            this.btnExecute.TabIndex = 4;
            this.btnExecute.Text = "Get Query!";
            this.btnExecute.UseVisualStyleBackColor = true;
            this.btnExecute.Click += new System.EventHandler(this.btnExecute_Click);
            // 
            // txtPrefSQL
            // 
            this.txtPrefSQL.Location = new System.Drawing.Point(514, 18);
            this.txtPrefSQL.Multiline = true;
            this.txtPrefSQL.Name = "txtPrefSQL";
            this.txtPrefSQL.Size = new System.Drawing.Size(426, 174);
            this.txtPrefSQL.TabIndex = 5;
            // 
            // gridSkyline
            // 
            this.gridSkyline.ColumnHeadersHeightSizeMode = System.Windows.Forms.DataGridViewColumnHeadersHeightSizeMode.AutoSize;
            this.gridSkyline.Dock = System.Windows.Forms.DockStyle.Bottom;
            this.gridSkyline.Location = new System.Drawing.Point(0, 343);
            this.gridSkyline.Name = "gridSkyline";
            this.gridSkyline.Size = new System.Drawing.Size(952, 275);
            this.gridSkyline.TabIndex = 7;
            // 
            // label1
            // 
            this.label1.AutoSize = true;
            this.label1.Location = new System.Drawing.Point(724, 286);
            this.label1.Name = "label1";
            this.label1.Size = new System.Drawing.Size(89, 13);
            this.label1.TabIndex = 8;
            this.label1.Text = "elapsed time algo";
            // 
            // txtTimeAlgo
            // 
            this.txtTimeAlgo.Location = new System.Drawing.Point(828, 283);
            this.txtTimeAlgo.Name = "txtTimeAlgo";
            this.txtTimeAlgo.ReadOnly = true;
            this.txtTimeAlgo.Size = new System.Drawing.Size(100, 20);
            this.txtTimeAlgo.TabIndex = 9;
            // 
            // optDQ
            // 
            this.optDQ.AutoSize = true;
            this.optDQ.Location = new System.Drawing.Point(24, 139);
            this.optDQ.Name = "optDQ";
            this.optDQ.Size = new System.Drawing.Size(65, 17);
            this.optDQ.TabIndex = 4;
            this.optDQ.TabStop = true;
            this.optDQ.Text = "D and Q";
            this.optDQ.UseVisualStyleBackColor = true;
            // 
            // txtTime
            // 
            this.txtTime.Location = new System.Drawing.Point(828, 257);
            this.txtTime.Name = "txtTime";
            this.txtTime.ReadOnly = true;
            this.txtTime.Size = new System.Drawing.Size(100, 20);
            this.txtTime.TabIndex = 11;
            // 
            // label2
            // 
            this.label2.AutoSize = true;
            this.label2.Location = new System.Drawing.Point(747, 260);
            this.label2.Name = "label2";
            this.label2.Size = new System.Drawing.Size(66, 13);
            this.label2.TabIndex = 10;
            this.label2.Text = "elapsed time";
            // 
            // txtRecords
            // 
            this.txtRecords.Location = new System.Drawing.Point(828, 309);
            this.txtRecords.Name = "txtRecords";
            this.txtRecords.ReadOnly = true;
            this.txtRecords.Size = new System.Drawing.Size(100, 20);
            this.txtRecords.TabIndex = 13;
            // 
            // label3
            // 
            this.label3.AutoSize = true;
            this.label3.Location = new System.Drawing.Point(771, 312);
            this.label3.Name = "label3";
            this.label3.Size = new System.Drawing.Size(42, 13);
            this.label3.TabIndex = 12;
            this.label3.Text = "records";
            // 
            // FrmSQLParser
            // 
            this.AutoScaleDimensions = new System.Drawing.SizeF(6F, 13F);
            this.AutoScaleMode = System.Windows.Forms.AutoScaleMode.Font;
            this.ClientSize = new System.Drawing.Size(952, 618);
            this.Controls.Add(this.txtRecords);
            this.Controls.Add(this.label3);
            this.Controls.Add(this.txtTime);
            this.Controls.Add(this.label2);
            this.Controls.Add(this.txtTimeAlgo);
            this.Controls.Add(this.label1);
            this.Controls.Add(this.gridSkyline);
            this.Controls.Add(this.txtPrefSQL);
            this.Controls.Add(this.btnExecute);
            this.Controls.Add(this.chkShowSkyline);
            this.Controls.Add(this.groupBox1);
            this.Name = "FrmSQLParser";
            this.Text = "FrmSQLParser";
            this.Load += new System.EventHandler(this.FrmSQLParser_Load);
            this.groupBox1.ResumeLayout(false);
            this.groupBox1.PerformLayout();
            ((System.ComponentModel.ISupportInitialize)(this.gridSkyline)).EndInit();
            this.ResumeLayout(false);
            this.PerformLayout();

        }

        #endregion

        private GroupBox groupBox1;
        private RadioButton optBNL;
        private RadioButton optSQL;
        private CheckBox chkShowSkyline;
        private Button btnExecute;
        private TextBox txtPrefSQL;
        private DataGridView gridSkyline;
        private RadioButton optHexagon;
        private Label label1;
        private TextBox txtTimeAlgo;
        private RadioButton optDQ;
        private TextBox txtTime;
        private Label label2;
        private TextBox txtRecords;
        private Label label3;
    }
}