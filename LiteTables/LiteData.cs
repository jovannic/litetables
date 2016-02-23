using System;
using System.Collections.Generic;
using System.Runtime.InteropServices;

namespace LiteTables {
    public static class LiteData {
        public static int ReadInt32(byte[] data, int startIndex) {
            return (data[startIndex + 3] << 24) | (data[startIndex + 2] << 16) | (data[startIndex + 1] << 8) | data[startIndex];
        }

        public static float ReadSingle(byte[] data, int startIndex) {
            var union = new Int32SingleUnion(ReadInt32(data, startIndex));
            return union.f;
        }

        public static int ReadInt32(byte[] data, int offset, ColumnType storageType, List<string> stringTable) {
            switch (storageType) {
                case ColumnType.Int32:
                    return ReadInt32(data, offset);
                case ColumnType.Single:
                    return (int)ReadSingle(data, offset);
                case ColumnType.String:
                    int index = ReadInt32(data, offset);
                    string s = index >= 0 ? stringTable[index] : null;
                    int value;
                    return int.TryParse(s, out value) ? value : 0;
                default:
                    throw new InvalidCastException();
            }
        }

        public static float ReadSingle(byte[] data, int offset, ColumnType storageType, List<string> stringTable) {
            switch (storageType) {
                case ColumnType.Int32:
                    return (float)ReadInt32(data, offset);
                case ColumnType.Single:
                    return ReadSingle(data, offset);
                case ColumnType.String:
                    int index = ReadInt32(data, offset);
                    string s = index >= 0 ? stringTable[index] : null;
                    float value;
                    return float.TryParse(s, out value) ? value : 0;
                default:
                    throw new InvalidCastException();
            }
        }

        public static string ReadString(byte[] data, int offset, ColumnType storageType, List<string> stringTable) {
            switch (storageType) {
                case ColumnType.Int32:
                    return ReadInt32(data, offset).ToString();
                case ColumnType.Single:
                    return ReadSingle(data, offset).ToString();
                case ColumnType.String:
                    int index = ReadInt32(data, offset);
                    string s = index >= 0 ? stringTable[index] : null;
                    return s;
                default:
                    throw new InvalidCastException();
            }
        }

        public static void WriteInt32(byte[] data, int startIndex, int value) {
            data[startIndex] = (byte)value;
            data[startIndex + 1] = (byte)(value >> 8);
            data[startIndex + 2] = (byte)(value >> 16);
            data[startIndex + 3] = (byte)(value >> 24);
        }

        public static void WriteSingle(byte[] data, int startIndex, float value) {
            var union = new Int32SingleUnion(value);
            WriteInt32(data, startIndex, union.i);
        }

        public static int GetLength(ColumnType type) {
            switch (type) {
                default:
                    return 4;
            }
        }

        [StructLayout(LayoutKind.Explicit)]
        private struct Int32SingleUnion {
            [FieldOffset(0)]
            public int i;
            [FieldOffset(0)]
            public float f;

            internal Int32SingleUnion(int i) {
                this.f = 0;
                this.i = i;
            }
            internal Int32SingleUnion(float f) {
                this.i = 0;
                this.f = f;
            }
        }
    }

    public class Column {
        public readonly string Name;

        public readonly ColumnType Type;
        public readonly int Offset;

        public Column(string name, ColumnType type, int offset) {
            Name = name;
            Type = type;
            Offset = offset;
        }
    }

    public enum ColumnType : byte {
        Int32 = 1,
        Single = 2,
        String = 3,
    }
}
