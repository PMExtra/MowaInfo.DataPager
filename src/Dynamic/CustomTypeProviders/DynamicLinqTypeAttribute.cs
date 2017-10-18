using System;

namespace MowaInfo.DataPager.Dynamic.CustomTypeProviders
{
    /// <summary>
    ///     Indicates to Dynamic Linq to consider the Type as a valid dynamic linq type.
    /// </summary>
    [AttributeUsage(AttributeTargets.Class | AttributeTargets.Enum, Inherited = false)]
    public sealed class DynamicLinqTypeAttribute : Attribute
    {
    }
}