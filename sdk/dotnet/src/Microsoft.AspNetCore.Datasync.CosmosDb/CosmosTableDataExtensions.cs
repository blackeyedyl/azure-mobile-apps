// Copyright (c) Microsoft Corporation. All Rights Reserved.
// Licensed under the MIT License.

using Microsoft.Azure.Cosmos;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;

namespace Microsoft.AspNetCore.Datasync.CosmosDb
{
    /// <summary>
    /// Extensions for <see cref="CosmosTableData"/>.
    /// </summary>
    public static class CosmosTableDataExtensions
    {
        /// <summary>
        /// List of types that can be converted to a <see cref="double"/>.
        /// </summary>
        internal static List<Type> doubleTypes = new()
        {
            typeof(byte),
            typeof(decimal),
            typeof(double),
            typeof(float),
            typeof(int),
            typeof(long),
            typeof(sbyte),
            typeof(short),
            typeof(uint),
            typeof(ulong),
            typeof(ushort)
        };

        /// <summary>
        /// Based on the <see cref="CosmosTableData"/> <paramref name="entity"/> try and 
        /// build a partition key based on the <paramref name="partitionKeyPropertyNames"/>.
        /// </summary>
        /// <param name="entity"></param>
        /// <param name="partitionKeyPropertyNames"></param>
        /// <returns></returns>
        /// <exception cref="ArgumentException"></exception>
        /// <exception cref="ArgumentNullException"></exception>
        public static PartitionKey BuildPartitionKey(this ITableData entity, List<string> partitionKeyPropertyNames)
        {
            ArgumentNullException.ThrowIfNull(partitionKeyPropertyNames, nameof(partitionKeyPropertyNames));
            if (!partitionKeyPropertyNames.Any()) 
            {
                throw new ArgumentException("partitionKeyPropertyNames is empty");
            }
            var partitionKeyBuilder = new PartitionKeyBuilder();

            foreach (var propertyName in partitionKeyPropertyNames)
            {
                PropertyInfo propertyInfo = entity.GetType().GetProperty(propertyName);
                if (propertyInfo == null)
                {
                    throw new ArgumentException($"Property '{propertyName}' not found on entity.");
                }
                
                var value = propertyInfo.GetValue(entity);
                if (value == null)
                {
                    throw new ArgumentNullException($"Value of property '{propertyName}' cannot be null.");
                }
                
                if (doubleTypes.Contains(value.GetType()))
                {
                    var doubleValue = Convert.ToDouble(value);
                    partitionKeyBuilder.Add(doubleValue);
                }
                else if (value is bool boolValue)
                {
                    partitionKeyBuilder.Add(boolValue);
                }
                else
                {
                    partitionKeyBuilder.Add(value.ToString());
                }
            }

            return partitionKeyBuilder.Build();
        }
    }
}
