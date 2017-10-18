using MowaInfo.DataPager.Dynamic.CustomTypeProviders;

namespace MowaInfo.DataPager.Dynamic
{
    /// <summary>
    ///     Static configuration class for Dynamic Linq.
    /// </summary>
    public static class GlobalConfig
    {
        private static IDynamicLinkCustomTypeProvider _customTypeProvider;

        private static bool _contextKeywordsEnabled = true;

        /// <summary>
        ///     Gets or sets the <see cref="IDynamicLinkCustomTypeProvider" />.
        /// </summary>
        public static IDynamicLinkCustomTypeProvider CustomTypeProvider
        {
            get => _customTypeProvider;

            set
            {
                if (_customTypeProvider != value)
                {
                    _customTypeProvider = value;

                    ExpressionParser.ResetDynamicLinqTypes();
                }
            }
        }

        /// <summary>
        ///     Determines if the context keywords (it, parent, and root) are valid and usable inside a Dynamic Linq string
        ///     expression.
        ///     Does not affect the usability of the equivalent context symbols ($, ^ and ~).
        /// </summary>
        public static bool AreContextKeywordsEnabled
        {
            get => _contextKeywordsEnabled;
            set
            {
                if (value != _contextKeywordsEnabled)
                {
                    _contextKeywordsEnabled = value;

                    ExpressionParser.ResetDynamicLinqTypes();
                }
            }
        }

        /// <summary>
        ///     Gets or sets a value indicating whether to use dynamic object class for anonymous types.
        /// </summary>
        /// <value>
        ///     <c>true</c> if wether to use dynamic object class for anonymous types; otherwise, <c>false</c>.
        /// </value>
        public static bool UseDynamicObjectClassForAnonymousTypes { get; set; }
    }
}