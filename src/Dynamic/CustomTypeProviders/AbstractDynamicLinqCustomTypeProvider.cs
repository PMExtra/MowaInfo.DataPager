using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;

namespace MowaInfo.DataPager.Dynamic.CustomTypeProviders
{
    /// <summary>
    ///     The abstract <see cref="AbstractDynamicLinqCustomTypeProvider" />. Find all types marked with
    ///     <see cref="DynamicLinqTypeAttribute" />.
    /// </summary>
    public abstract class AbstractDynamicLinqCustomTypeProvider
    {
        /// <summary>
        ///     Finds the types marked with DynamicLinqTypeAttribute.
        /// </summary>
        /// <param name="assemblies">The assemblies to process.</param>
        /// <returns>IEnumerable{Type}</returns>
        protected IEnumerable<Type> FindTypesMarkedWithDynamicLinqTypeAttribute(IEnumerable<Assembly> assemblies)
        {
            assemblies = assemblies.Where(x => !x.IsDynamic);
            var definedTypes = GetAssemblyTypes(assemblies);

            return definedTypes
                .Where(x => x.CustomAttributes.Any(y => y.AttributeType == typeof(DynamicLinqTypeAttribute)))
                .Select(x => x.AsType());
        }


        /// <summary>
        ///     Gets the assembly types in an Exception friendly way.
        /// </summary>
        /// <param name="assemblies">The assemblies to process.</param>
        /// <returns>IEnumerable{Type}</returns>
        protected IEnumerable<TypeInfo> GetAssemblyTypes(IEnumerable<Assembly> assemblies)
        {
            foreach (var assembly in assemblies)
            {
                IEnumerable<TypeInfo> definedTypes = null;

                try
                {
                    definedTypes = assembly.DefinedTypes;
                }
                catch (Exception)
                {
                }

                if (definedTypes != null)
                    foreach (var definedType in definedTypes)
                        yield return definedType;
            }
        }
    }
}