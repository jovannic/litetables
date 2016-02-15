using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using LiteTables;
using LiteTables.Convert;
using System.IO;
using System.Diagnostics;

namespace LiteTables.Test {
    [TestClass]
    public class ConversionTests {
        [TestMethod]
        public void TinytestConvertVerify() {
            string infile = "tinytest.db";
            string outfile = "tinytest.dat";

            SQLiteConverter.Convert(infile, outfile);

            var stream = File.Open(outfile, FileMode.Open);
            var reader = new LiteReader(stream);

            int stringCount = reader.GetStringCount();
            Console.WriteLine("{0} strings", stringCount);
            for (int i = 0; i < stringCount; i++) {
                Console.WriteLine(reader.GetString(i));
            }

            string[] names = new string[] { "apple", "banana", "carrot" };
            int rowID = 1;
            foreach (var row in reader.GetTableRows("table1", "id", "name", "floating")) {
                int id = row.ReadInt32(0);
                string name = row.ReadString(1);
                float floating = row.ReadSingle(2);

                Assert.AreEqual(rowID, id);
                Assert.AreEqual(name, names[rowID - 1]);
                Assert.AreEqual(rowID + 0.5f, floating);
                rowID++;
            }

            rowID = 1;
            foreach (var row in reader.GetTableRows("table2", "id", "name")) {
                int id = row.ReadInt32(0);
                string name = row.ReadString(1);

                Assert.AreEqual(rowID, 1); // only 1 row
                Assert.AreEqual(rowID, id);
                Assert.AreEqual(name, "pear");
                rowID++;
            }
        }
    }
}
