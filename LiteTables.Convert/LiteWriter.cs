using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Text;

namespace LiteTables.Convert {
    public class LiteWriter {
        private Dictionary<string, int> stringIndexes;
        private List<string> stringTable;

        private List<LiteTableBuilder> tables = new List<LiteTableBuilder>();

        public LiteWriter() {
            int initialCapacity = 1024;
            stringTable = new List<string>(initialCapacity);
            stringIndexes = new Dictionary<string, int>(initialCapacity);
        }

        public LiteTableBuilder AddTable(string name, Column[] columns, int rowLength) {
            if (!tables.TrueForAll(t => t.Name != name))
                throw new Exception();

            var table = new LiteTableBuilder(this, name, columns, rowLength);
            tables.Add(table);

            AddString(name);
            foreach (var column in columns) {
                AddString(column.Name);
            }

            return table;
        }

        public int AddString(string value) {
            if (value == null) {
                return -1;
            }

            int index;
            if (!stringIndexes.TryGetValue(value, out index)) {
                index = stringTable.Count;
                stringTable.Add(value);
                stringIndexes[value] = index;
            }
            return index;
        }

        public void Write(string fileName) {
            using (var stream = File.Open(fileName, FileMode.Create))
            using (var buffered = new BufferedStream(stream)) {
                Write(buffered);
            }
        }

        public void Write(Stream stream) {
            using (var writer = new BinaryWriter(stream)) {
                Write(writer);
                writer.Flush();
            }
        }

        private void Write(BinaryWriter writer) {
            List<string> strings = stringTable;
            List<byte[]> stringBytes = new List<byte[]>(strings.Count);

            foreach (var str in strings) {
                stringBytes.Add(Encoding.UTF8.GetBytes(str));
            }

            // string count
            int stringCount = stringBytes.Count;
            writer.Write(stringCount);

            Trace.WriteLine("stringCount: " + stringCount);

            // string offset
            int offset = 0;
            foreach (var bytes in stringBytes) {
                int length = bytes.Length;

                writer.Write(offset);
                writer.Write(length);

                offset += length;
            }

            // string block: data length
            writer.Write(offset);
            // string block: data
            foreach (var bytes in stringBytes) {
                writer.Write(bytes, 0, bytes.Length);
            }

            // table count
            int tableCount = tables.Count;
            writer.Write(tableCount);

            Trace.WriteLine("tableCount: " + tableCount);

            offset = 0;
            foreach (var table in tables) {
                var columns = table.Columns;
                var rows = table.rows;

                int rowCount = rows.Count;
                int rowLength = table.rowLength;

                writer.Write(AddString(table.Name));
                writer.Write(rowCount);
                writer.Write(rowLength);

                int columnCount = columns.Length;
                writer.Write(columnCount);
                foreach (var column in columns) {
                    writer.Write(AddString(column.Name));
                    writer.Write((byte)column.Type);
                }

                writer.Write(rowCount * rowLength);
                foreach (var row in rows) {
                    if (row.Length != rowLength)
                        throw new Exception();

                    writer.Write(row);
                }
            }
        }
    }

    public class LiteTableBuilder {
        public readonly string Name;

        public List<byte[]> rows = new List<byte[]>();
        public int rowLength;

        public Column[] Columns { get; private set; }

        public LiteWriter Writer { get; private set; }

        public LiteTableBuilder(LiteWriter writer, string name, Column[] columns, int rowLength) {
            Name = name;
            this.Columns = columns;
            this.rowLength = rowLength;
            Writer = writer;
        }

        public RowBuilder AddRow() {
            var data = new byte[rowLength];
            rows.Add(data);

            var row = new RowBuilder(this, data);
            return row;
        }

        public class RowBuilder {
            LiteTableBuilder table;
            private byte[] data;

            public RowBuilder(LiteTableBuilder table, byte[] data) {
                this.table = table;
                this.data = data;
            }

            public void Write(int columnIndex, string value) {
                var column = table.Columns[columnIndex];
                if (column.Type != ColumnType.String) {
                    throw new InvalidCastException();
                }

                int index = table.Writer.AddString(value);
                LiteData.WriteInt32(data, column.Offset, index);
            }

            public void Write(int columnIndex, int value) {
                var column = table.Columns[columnIndex];
                if (column.Type != ColumnType.Int32) {
                    throw new InvalidCastException();
                }

                LiteData.WriteInt32(data, column.Offset, value);
            }

            public void Write(int columnIndex, float value) {
                var column = table.Columns[columnIndex];
                if (column.Type != ColumnType.Single) {
                    throw new InvalidCastException();
                }

                LiteData.WriteSingle(data, column.Offset, value);
            }
        }
    }
}
