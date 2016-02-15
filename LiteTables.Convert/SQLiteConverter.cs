using SQLite;
using SQLitePCL;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Text;

namespace LiteTables.Convert {
    public class SQLiteConverter {
        static void Main(string[] args) {
            Convert(args[0], args[1]);
        }

        public static void Convert(string filename, string outFilename) {
            Trace.WriteLine("Opening " + filename);

            sqlite3 handle;
            SQLite3.Result result = SQLite3.Open(filename, out handle);

            try {
                if (result != SQLite3.Result.OK) {
                    string msg = SQLite3.GetErrmsg(handle);
                    throw new Exception(msg);
                }

                Trace.WriteLine("Opened");

                List<string> tableNames = new List<string>();
                var writer = new LiteWriter();

                GetTableNames(handle, tableNames);
                // build the table definitions, add the data
                CreateTables(handle, tableNames, writer);

                writer.Write(outFilename);
            } finally {
                if (handle != null) {
                    SQLite3.Close(handle);
                    handle = null;
                }
            }
        }

        private static void GetTableNames(sqlite3 handle, List<string> tableNames) {
            var stmt = SQLite3.Prepare2(handle, "SELECT name FROM sqlite_master WHERE type = 'table' ORDER BY name");
            try {
                while (SQLite3.Step(stmt) == SQLite3.Result.Row) {
                    string name = SQLite3.ColumnString(stmt, 0);

                    if (name != "sqlite_sequence")
                        tableNames.Add(name);
                }
            } finally {
                SQLite3.Finalize(stmt);
            }
        }

        private static void CreateTables(sqlite3 handle, List<string> tableNames, LiteWriter writer) {
            foreach (var tableName in tableNames) {
                var stmt = SQLite3.Prepare2(handle, "pragma table_info(" + tableName + ")");
                try {
                    int offset = 0;
                    List<Column> columns = new List<Column>();
                    while (SQLite3.Step(stmt) == SQLite3.Result.Row) {
                        string name = SQLite3.ColumnString(stmt, 1);
                        string type = SQLite3.ColumnString(stmt, 2);
                        int notNull = SQLite3.ColumnInt(stmt, 3);

                        var ct = GetType(type);
                        var column = new Column(name, ct, offset);
                        columns.Add(column);

                        offset += LiteData.GetLength(ct);
                    }

                    var columnArray = new Column[columns.Count];
                    columns.CopyTo(columnArray);

                    var table = writer.AddTable(tableName, columnArray, offset);

                    AddRows(handle, table);
                } finally {
                    SQLite3.Finalize(stmt);
                }
            }
        }

        private static void AddRows(sqlite3 handle, LiteTableBuilder table) {
            var columns = table.Columns;
            string query = GetTableQuery(table.Name, Array.ConvertAll(columns, c => c.Name));
            var stmt = SQLite3.Prepare2(handle, query);
            try {
                while (SQLite3.Step(stmt) == SQLite3.Result.Row) {
                    var row = table.AddRow();
                    for (int i = 0; i < columns.Length; i++) {
                        var column = columns[i];
                        switch (column.Type) {
                            case ColumnType.Int32:
                                row.Write(i, SQLite3.ColumnInt(stmt, i));
                                break;
                            case ColumnType.Single:
                                row.Write(i, (float)SQLite3.ColumnDouble(stmt, i));
                                break;
                            case ColumnType.String:
                                row.Write(i, SQLite3.ColumnString(stmt, i));
                                break;
                        }
                    }
                }
            } finally {
                SQLite3.Finalize(stmt);
            }
        }

        private static string GetTableQuery(string tableName, string[] columns) {
            StringBuilder query = new StringBuilder("select ", 512);

            // add columns, in order
            foreach (string col in columns)
                query.Append(col).Append(", ");

            // remove last comma
            if (columns.Length > 0)
                query.Length -= 2;

            // from table
            query.Append(" from ").Append(tableName);

            return query.ToString();
        }

        private static Dictionary<string, ColumnType> typeMap = new Dictionary<string, ColumnType>() {
            {"int", ColumnType.Int32},
            {"integer", ColumnType.Int32},
            {"tinyint", ColumnType.Int32},
            {"text", ColumnType.String},
            {"varchar", ColumnType.String},
            {"real", ColumnType.Single},
            {"numeric", ColumnType.Single},
            {"bigint", ColumnType.Int32},
        };

        public static ColumnType GetType(string type) {
            int paren = type.IndexOf("(");
            if (paren != -1) {
                type = type.Substring(0, paren);
            }

            ColumnType ct;
            if (typeMap.TryGetValue(type.ToLowerInvariant(), out ct)) {
                return ct;
            } else {
                throw new InvalidCastException("no ColumnType for " + type);
            }
        }
    }
}
