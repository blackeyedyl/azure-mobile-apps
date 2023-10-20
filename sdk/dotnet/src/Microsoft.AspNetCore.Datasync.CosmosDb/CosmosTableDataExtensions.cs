// Copyright (c) Microsoft Corporation. All Rights Reserved.
// Licensed under the MIT License.

using Microsoft.Azure.Cosmos;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;

namespace Microsoft.AspNetCore.Datasync.CosmosDb
{
    public static class CosmosTableDataExtensions
    {
        /// <summary>
        /// Converts the entity's ID and to a compound ID that includes the partition key (e.g. "id:partitionKey1|partitionKey2").
        /// </summary>
        /// <param name="entity"></param>
        /// <param name="partitionKeyPropertyNames"></param>
        /// <returns></returns>
        /// <exception cref="ArgumentException"></exception>
        /// <exception cref="ArgumentNullException"></exception>
        public static string TranslateToDtoId(this ITableData entity, List<string> partitionKeyPropertyNames)
        {
            ArgumentNullException.ThrowIfNull(partitionKeyPropertyNames, nameof(partitionKeyPropertyNames));
            if (!partitionKeyPropertyNames.Any())
            {
                throw new ArgumentException("partitionKeyPropertyNames is empty");
            }
            var result = entity.Id;
            if (partitionKeyPropertyNames.Count == 1 && partitionKeyPropertyNames[0].Equals("id", StringComparison.OrdinalIgnoreCase))
            {
                return result;
            }
            List<string> values = new();
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
                values.Add(value.ToString());
            }
            return $"{entity.Id}:{string.Join('|', values)}";
        }

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

                switch (value)
                {
                    case double doubleValue:
                        partitionKeyBuilder.Add(doubleValue);
                        break;
                    case float floatValue:
                        partitionKeyBuilder.Add(floatValue);
                        break;
                    case decimal decimalValue:
                        partitionKeyBuilder.Add((double)decimalValue);
                        break;
                    case int intValue:
                        partitionKeyBuilder.Add(intValue);
                        break;
                    case uint uintValue:
                        partitionKeyBuilder.Add(uintValue);
                        break;
                    case bool boolValue:
                        partitionKeyBuilder.Add(boolValue);
                        break;
                    default:
                        partitionKeyBuilder.Add(value.ToString());
                        break;
                }
            }

            return partitionKeyBuilder.Build();
        }
    }
}
