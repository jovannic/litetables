using System;
using System.Collections.Generic;
using System.Text;
using System.Runtime.InteropServices;
using System.IO;

namespace LiteTables {
    public class LiteReader {
        private List<string> stringTable;
        private Dictionary<string, Table> tables;

        public LiteReader(Stream stream) {
            using (var reader = new BinaryReader(stream)) {
                ReadStringTable(reader);
                ReadTableSchemas(reader);
            }
        }

        private void ReadStringTable(BinaryReader reader) {
            int stringCount = reader.ReadInt32();

            stringTable = new List<string>(stringCount);

            StringBound[] bounds = new StringBound[stringCount];
            for (int i = 0; i < stringCount; i++) {
                var bound = new StringBound();
                bound.Offset = reader.ReadInt32();
                bound.Length = reader.ReadInt32();
                bounds[i] = bound;
            }

            var builder = new StringBuilder();
            var decoder = Encoding.UTF8.GetDecoder();
            var byteBuffer = new byte[1024];
            var charBuffer = new char[1024];

            int stringDataLength = reader.ReadInt32();
            int lastEndOffset = 0;
            for (int i = 0; i < stringCount; i++) {
                var bound = bounds[i];

                if (bound.Offset < lastEndOffset) {
                    // don't allow overlap
                    throw new IndexOutOfRangeException("Two string ranges overlapped.");
                } else if (bound.Offset > lastEndOffset) {
                    // skip padding (?) bytes
                    reader.BaseStream.Position += bound.Offset - lastEndOffset;
                }

                int thisEndOffset = bound.Offset + bound.Length;
                if (thisEndOffset > stringDataLength) {
                    throw new IndexOutOfRangeException("String end exceded data length.");
                }

                // The string length is in bytes, so we can't use StreamReader
                // We need to control how many bytes are read.
                int remainingBytes = bound.Length;
                if (remainingBytes > 0) {
                    int readBytes = 0;
                    bool completedString = false;
                    while ((readBytes = reader.Read(byteBuffer, 0, Math.Min(byteBuffer.Length, remainingBytes))) != 0) {
                        remainingBytes -= readBytes;

                        int charCount, byteCount;
                        decoder.Convert(byteBuffer, 0, readBytes, charBuffer, 0, charBuffer.Length, remainingBytes == 0, out byteCount, out charCount, out completedString);
                        builder.Append(charBuffer, 0, charCount);

                        if (remainingBytes == 0) {
                            break;
                        }
                    }

                    if (remainingBytes != 0 || !completedString) {
                        throw new InvalidDataException("Failed to fully read a string.");
                    }

                    decoder.Reset();
                    string str = builder.ToString();
                    builder.Length = 0;

                    stringTable.Add(str);
                } else {
                    stringTable.Add(string.Empty);
                }

                lastEndOffset = thisEndOffset;
            }
        }

        private void ReadTableSchemas(BinaryReader reader) {
            int tableCount = reader.ReadInt32();

            tables = new Dictionary<string, Table>(tableCount);

            for (int i = 0; i < tableCount; i++) {
                int nameIndex = reader.ReadInt32();
                int rowCount = reader.ReadInt32();
                int rowLength = reader.ReadInt32();

                string name = GetString(nameIndex);

                int offset = 0;
                int columnCount = reader.ReadInt32();
                Dictionary<string, Column> columns = new Dictionary<string, Column>(columnCount);
                for (int j = 0; j < columnCount; j++) {
                    int columnNameIndex = reader.ReadInt32();
                    byte dataType = reader.ReadByte();

                    string columnName = GetString(columnNameIndex);
                    ColumnType type = (ColumnType)dataType;

                    var column = new Column(columnName, type, offset);
                    columns[columnName] = column;

                    offset += LiteData.GetLength(type);
                }

                int length = reader.ReadInt32();
                byte[] tableData = reader.ReadBytes(length);

                var table = new Table(name, columns, rowLength, rowCount, tableData);
                tables.Add(name, table);
            }
        }

        public string GetString(int index) {
            return stringTable[index];
        }

        public int GetStringCount() {
            return stringTable.Count;
        }
        
        // This is the important function
        public IEnumerable<Row> GetTableRows(string tableName, params string[] columnNames) {
            Table table;
            if (tables.TryGetValue(tableName, out table) && table.RowCount > 0) {
                // Create a reader with the columns in the specified order
                var columns = new Column[columnNames.Length];
                for (int i = 0; i < columnNames.Length; i++) {
                    Column column;
                    if (table.Columns.TryGetValue(columnNames[i], out column)) {
                        columns[i] = column;
                    } else {
                        // Uses null checks for default values.
                        // It was that or using virtual method.
                        columns[i] = null;
                    }
                }

                using (var reader = new Row(table.Data, columns, stringTable)) {
                    int offset = 0;
                    int count = table.RowCount;
                    for (int i = 0; i < count; i++) {
                        reader.Reset(offset);
                        yield return reader;

                        offset += table.RowLength;
                    }
                }
            } else {
                // Empty (or missing) table silently has no rows
                yield break;
            }
        }

        private struct StringBound {
            public int Offset;
            public int Length;
        }

        private class Table {
            public string Name;
            public Dictionary<string, Column> Columns;
            public int RowLength;
            public int RowCount;

            public byte[] Data;

            public Table(string name, Dictionary<string, Column> columns, int rowLength, int rowCount, byte[] tableData) {
                Name = name;
                Columns = columns;
                this.RowLength = rowLength;
                RowCount = rowCount;
                this.Data = tableData;
            }
        }

        public class Row : IDisposable {
            private List<string> stringTable;
            private Column[] columns;

            private byte[] data;
            private int offset = 0;

            public Row(byte[] data, Column[] columns, List<string> stringTable) {
                this.data = data;
                this.columns = columns;
                this.stringTable = stringTable;
            }

            public void Reset(int rowStartOffset) {
                this.offset = rowStartOffset;
            }

            public int ReadInt32(int columnIndex) {
                var column = columns[columnIndex];
                return column != null ? LiteData.ReadInt32(data, offset + column.Offset, column.Type, stringTable) : 0;
            }

            public string ReadString(int columnIndex) {
                var column = columns[columnIndex];
                return column != null ? LiteData.ReadString(data, offset + column.Offset, column.Type, stringTable) : null;
            }

            public float ReadSingle(int columnIndex) {
                var column = columns[columnIndex];
                return column != null ? LiteData.ReadSingle(data, offset + column.Offset, column.Type, stringTable) : 0.0f;
            }

            public void Dispose() {
                data = null;
                columns = null;
                stringTable = null;
            }
        }
    }

}
