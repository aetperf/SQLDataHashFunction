using System;
using System.Data.SqlTypes;
using Microsoft.SqlServer.Server;


using System.Diagnostics;
using System.Runtime.CompilerServices;


namespace SQLDataHashFunctions
{
    public partial class FNV1A
    {
        static readonly ulong prime64 = 1099511628211;
        static readonly ulong offset64 = 0xcbf29ce484222325;
        static readonly uint prime32 = 16777619;
        static readonly uint offset32 = 2166136261;




        [SqlFunction(IsDeterministic = true, IsPrecise = true, DataAccess = DataAccessKind.None)]
        public static SqlInt64 XF_HashFnv1a_64(SqlBinary value) => HashFNV1a_64(value);

        [SqlFunction(IsDeterministic = true, IsPrecise = true, DataAccess = DataAccessKind.None)]
        public static SqlInt32 XF_HashFnv1a_32(SqlBinary value) => HashFNV1a_32(value);

        [SqlFunction(IsDeterministic = true, IsPrecise = true, DataAccess = DataAccessKind.None)]
        public static SqlInt16 XF_HashFnv1a_16(SqlBinary value) => HashFNV1a_16(value);


        private static long HashFNV1a_64(SqlBinary bytes)
        {
            ulong hash = offset64;
            //byte[] bytes = Encoding.UTF8.GetBytes(value.ToString());
            //byte[] bytes = value.GetUnicodeBytes();
            for (int i = 0; i < bytes.Length; i++)
            {
                ulong v = (hash ^ bytes[i]);
                hash = v * prime64;
            }
            return (long)(hash - long.MaxValue);
        }

        private static int HashFNV1a_32(SqlBinary bytes)
        {
            uint hash = offset32;
            //byte[] bytes = value.GetUnicodeBytes();
            for (int i = 0; i < bytes.Length; i++)
            {
                uint v = (hash ^ bytes[i]);
                hash = v * prime32;
            }
            return (int)(hash - int.MaxValue);
        }
        private static short HashFNV1a_16(SqlBinary bytes)
        {
            uint MASK_16 = (((uint)1 << 16) - 1);    /* i.e., (u_int32_t)0xffff */
            uint hash = offset32;
            //byte[] bytes = value.GetUnicodeBytes();
            for (int i = 0; i < bytes.Length; i++)
            {
                uint v = (hash ^ bytes[i]);
                hash = v * prime32;
            }
            hash = (hash >> 16) ^ (hash & MASK_16);
            return (short)(hash - short.MaxValue);
        }

    };

    public partial class MURMUR
    {

        const UInt32 seed = 42; /* Define your own seed here */



        [SqlFunction(IsDeterministic = true, DataAccess = DataAccessKind.None)]
        public static SqlInt16 XF_MurMurBucket(SqlBinary data, SqlInt16 numBuckets) => HashBucket_16(data, numBuckets);


        [Microsoft.SqlServer.Server.SqlFunction(IsDeterministic = true, IsPrecise = false, DataAccess = DataAccessKind.None)]
        public static SqlInt32 XF_HashMurmur2_32(SqlBinary data) => HashMurmur2_32(data);

        [Microsoft.SqlServer.Server.SqlFunction(IsDeterministic = true, IsPrecise = false, DataAccess = DataAccessKind.None)]
        public static SqlInt64 XF_HashMurmur2_64(SqlBinary data) => HashMurmur2_64(data);

        [Microsoft.SqlServer.Server.SqlFunction(IsDeterministic = true, IsPrecise = true, DataAccess = DataAccessKind.None)]
        public static SqlInt32 XF_HashMurmur3_32(SqlBinary data) => HashMurmur3_32(data);

        [Microsoft.SqlServer.Server.SqlFunction(IsDeterministic = true, IsPrecise = true, DataAccess = DataAccessKind.None)]
        public static SqlInt64 XF_HashMurmur3_64(SqlBinary data) => HashMurmur3_64(data);


        
        private static SqlInt32 HashMurmur2_32(SqlBinary data)
        {

            if (data.ToString().Length == 0)
            {
                return SqlInt32.Null;
            }
            const UInt32 m = 0x5bd1e995;
            const Int32 r = 24;


            Int32 length = (Int32)data.Length;
            if (length == 0)
                return 0;
            UInt32 h = seed ^ (UInt32)length;
            Int32 currentIndex = 0;
            while (length >= 4)
            {
                UInt32 k = (UInt32)(data[currentIndex++]
                  | data[currentIndex++] << 8
                  | data[currentIndex++] << 16
                  | data[currentIndex++] << 24);
                k *= m;
                k ^= k >> r;
                k *= m;

                h *= m;
                h ^= k;
                length -= 4;
            }
            switch (length)
            {
                case 3:
                    h ^= (UInt16)(data[currentIndex++] | data[currentIndex++] << 8);
                    h ^= (UInt32)(data[currentIndex] << 16);
                    h *= m;
                    break;
                case 2:
                    h ^= (UInt16)(data[currentIndex++] | data[currentIndex] << 8);
                    h *= m;
                    break;
                case 1:
                    h ^= data[currentIndex];
                    h *= m;
                    break;
                default:
                    break;
            }

            // Do a few final mixes of the hash to ensure the last few
            // bytes are well-incorporated.

            h ^= h >> 13;
            h *= m;
            h ^= h >> 15;

            /* Interface back to SQL server */
            unchecked
            {
                return (SqlInt32)(Int32)h;
            }
        }

        private static SqlInt64 HashMurmur2_64(SqlBinary data)
        {

            //if (data.ToString().Length==0)
            //{
            //    return SqlInt64.Null;
            //}
            const UInt64 m = 0xc6a4a7935bd1e995;
            const Int32 r = 47;


            Int32 length = (Int32)data.Length;
            if (length == 0)
                return SqlInt64.Null;

            UInt64 h = seed ^ (UInt32)length;
            Int32 currentIndex = 0;
            while (length >= 4)
            {
                UInt64 k = (UInt32)(data[currentIndex++]
                  | data[currentIndex++] << 8
                  | data[currentIndex++] << 16
                  | data[currentIndex++] << 24);
                k *= m;
                k ^= k >> r;
                k *= m;

                h *= m;
                h ^= k;
                length -= 4;
            }
            switch (length)
            {
                case 3:
                    h ^= (UInt16)(data[currentIndex++] | data[currentIndex++] << 8);
                    h ^= (UInt32)(data[currentIndex] << 16);

                    break;
                case 2:
                    h ^= (UInt16)(data[currentIndex++] | data[currentIndex] << 8);

                    break;
                case 1:
                    h ^= data[currentIndex];

                    break;
                default:
                    break;
            }
            h *= m;

            // Do a few final mixes of the hash to ensure the last few
            // bytes are well-incorporated.

            h ^= h >> 13;
            h *= m;
            h ^= h >> 15;

            /* Interface back to SQL server */
            unchecked
            {
                return (SqlInt64)(Int64)h;
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static UInt32 rotl32(UInt32 x, byte r)
        {
            return (x << r) | (x >> (32 - r));
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static UInt64 rotl32(UInt64 x, byte r)
        {
            return (x << r) | (x >> (64 - r));
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static UInt32 fmix(UInt32 h)
        {
            h ^= h >> 16;
            h *= 0x85ebca6b;
            h ^= h >> 13;
            h *= 0xc2b2ae35;
            h ^= h >> 16;
            return h;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static UInt64 fmix(UInt64 h)
        {
            h ^= h >> 33;
            h *= 0xff51afd7ed558ccdul;
            h ^= h >> 33;
            h *= 0xc4ceb9fe1a85ec53ul;
            h ^= h >> 33;
            return h;
        }

        private static SqlInt32 HashMurmur3_32(SqlBinary data)
        {
            const UInt32 c1 = 0xcc9e2d51;
            const UInt32 c2 = 0x1b873593;
            if (data.ToString().Length == 0)
            {
                return SqlInt32.Null;
            }


            int curLength = data.Length;    /* Current position in byte array */
            int length = curLength;         /* the const length we need to fix tail */
            UInt32 h1 = seed;
            UInt32 k1 = 0;

            /* body, eat stream a 32-bit int at a time */
            Int32 currentIndex = 0;
            while (curLength >= 4)
            {
                /* Get four bytes from the input into an UInt32 */
                k1 = (UInt32)(data[currentIndex++]
                  | data[currentIndex++] << 8
                  | data[currentIndex++] << 16
                  | data[currentIndex++] << 24);

                /* bitmagic hash */
                k1 *= c1;
                k1 = rotl32(k1, 15);
                k1 *= c2;

                h1 ^= k1;
                h1 = rotl32(h1, 13);
                h1 = h1 * 5 + 0xe6546b64;
                curLength -= 4;
            }

            /* tail, the reminder bytes that did not make it to a full int */
            /* (this switch is slightly more ugly than the C++ implementation 
             * because we can't fall through) */




            switch (curLength)
            {
                case 3:
                    k1 ^= (UInt32)(data[currentIndex++]
                      | data[currentIndex++] << 8
                      | data[currentIndex++] << 16);
                    k1 *= c1;
                    k1 = rotl32(k1, 15);
                    k1 *= c2;
                    h1 ^= k1;
                    break;
                case 2:
                    k1 ^= (UInt32)(data[currentIndex++]
                      | data[currentIndex++] << 8);
                    k1 *= c1;
                    k1 = rotl32(k1, 15);
                    k1 *= c2;
                    h1 ^= k1;
                    break;
                case 1:
                    k1 ^= (UInt32)(data[currentIndex++]);
                    k1 *= c1;
                    k1 = rotl32(k1, 15);
                    k1 *= c2;
                    h1 ^= k1;
                    break;
            };

            // finalization, magic chants to wrap it all up
            h1 ^= (UInt32)length;
            h1 = fmix(h1);

            unchecked
            {
                return (SqlInt32)(Int32)h1;
            }
        }

        private static SqlInt64 HashMurmur3_64(SqlBinary data)
        {
            const UInt64 c1 = 0x87c37b91114253d5ul;
            const UInt64 c2 = 0x4cf5ad432745937ful;
            if (data.ToString().Length == 0)
            {
                return SqlInt64.Null;
            }


            int curLength = data.Length;    /* Current position in byte array */
            int length = curLength;   /* the const length we need to fix tail */
            UInt64 h1 = seed;
            UInt64 k1 = 0;

            /* body, eat stream a 32-bit int at a time */
            Int32 currentIndex = 0;
            while (curLength >= 4)
            {
                /* Get four bytes from the input into an UInt32 */
                k1 = (UInt64)(data[currentIndex++]
                  | data[currentIndex++] << 8
                  | data[currentIndex++] << 16
                  | data[currentIndex++] << 24);

                /* bitmagic hash */
                k1 *= c1;
                k1 = rotl32(k1, 15);
                k1 *= c2;

                h1 ^= k1;
                h1 = rotl32(h1, 13);
                h1 = h1 * 5 + 0xe6546b64;
                curLength -= 4;
            }

            /* tail, the reminder bytes that did not make it to a full int */
            /* (this switch is slightly more ugly than the C++ implementation 
             * because we can't fall through) */




            switch (curLength)
            {
                case 3:
                    k1 ^= (UInt64)(data[currentIndex++]
                      | data[currentIndex++] << 8
                      | data[currentIndex++] << 16);
                    k1 *= c1;
                    k1 = rotl32(k1, 15);
                    k1 *= c2;
                    h1 ^= k1;
                    break;
                case 2:
                    k1 ^= (UInt64)(data[currentIndex++]
                      | data[currentIndex++] << 8);
                    k1 *= c1;
                    k1 = rotl32(k1, 15);
                    k1 *= c2;
                    h1 ^= k1;
                    break;
                case 1:
                    k1 ^= (UInt64)(data[currentIndex++]);
                    k1 *= c1;
                    k1 = rotl32(k1, 15);
                    k1 *= c2;
                    h1 ^= k1;
                    break;
            };

            // finalization, magic chants to wrap it all up
            h1 ^= (UInt64)length;
            h1 = fmix(h1);

            unchecked
            {
                return (SqlInt64)(Int64)h1;
            }
        }

        private static SqlInt16 HashBucket_16(SqlBinary data, SqlInt16 numBuckets)
        {           
            SqlInt32 hashValue = HashMurmur3_32(data);
            SqlInt16 bucketId = Math.Abs((Int16)(hashValue % (Int16)numBuckets.Value));
            return (bucketId);
        }

    }

    public static partial class xxHash3
    {
        // WIP
    }
}





