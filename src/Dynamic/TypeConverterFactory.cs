using System;
using System.ComponentModel;

namespace MowaInfo.DataPager.Dynamic
{
    internal static class TypeConverterFactory
    {
        /// <summary>
        ///     Returns a type converter for the specified type.
        /// </summary>
        /// <param name="type">The System.Type of the target component.</param>
        /// <returns>A System.ComponentModel.TypeConverter for the specified type.</returns>
        public static TypeConverter GetConverter(Type type)
        {
            return TypeDescriptor.GetConverter(type);
        }
    }
}
