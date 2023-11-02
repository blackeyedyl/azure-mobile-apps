// Copyright (c) Microsoft Corporation. All Rights Reserved.
// Licensed under the MIT License.

using Newtonsoft.Json;
using System;
using System.Linq;
using System.Text;

namespace Microsoft.AspNetCore.Datasync.CosmosDb
{
    /// <summary>
    /// An implementation of <see cref="ITableData"/> that is appropriate
    /// for Cosmos databases that have an ETag (string-based) versioning
    /// concurrency check instead of a byte[] based versioning concurrency
    /// check
    /// </summary>
    public class CosmosTableData : ITableData
    {
        /// <summary>
        /// True if the entity is marked as deleted.
        /// </summary>
        public bool Deleted { get; set; }

        /// <summary>
        /// The ETag for the entity.
        /// </summary>
        [JsonProperty("_etag")]
        public string EntityTag { get; set; }

        /// <summary>
        /// The globally unique ID for this entity.
        /// </summary>
        [JsonProperty("id")]
        public string Id
        {
            get => TranslateToId();
            set => TranslateFromId(value);
        }

        /// <summary>
        /// Gets or sets the Cosmos database lookup ID for this entity.
        /// </summary>
        [JsonIgnore]
        public string LookupId { get; private set; }

        /// <summary>
        /// The date/time that the entity was updated.
        /// </summary>
        public DateTimeOffset UpdatedAt { get; set; } = DateTimeOffset.UtcNow;

        /// <summary>
        /// The row version for the entity.
        /// </summary>
        [JsonIgnore]
        public byte[] Version
        {
            get => Encoding.UTF8.GetBytes(EntityTag ?? string.Empty);
            set { EntityTag = Encoding.UTF8.GetString(value); }
        }

        /// <summary>
        /// Implements the <see cref="IEquatable{T}"/> interface to determine
        /// if the system properties match the system properties of the other
        /// entity.
        /// </summary>
        /// <param name="other">The other entity</param>
        /// <returns>true if the entity matches and the system properties are set.</returns>
        public bool Equals(ITableData other)
            => other != null
            && Id == other.Id
            && UpdatedAt == other.UpdatedAt
            && Deleted == other.Deleted
            && Version.SequenceEqual(other.Version);

        /// <summary>
        /// Translates the ID from the Cosmos ID to the Lookup ID.
        /// </summary>
        /// <param name="cosmosId"></param>
        protected virtual void TranslateFromId(string cosmosId)
        {
            if (cosmosId?.Contains(':') == true)
            {
                LookupId = cosmosId.Split(':')[0];
            }
            else
            {
                LookupId = cosmosId;
            }
        }

        /// <summary>
        /// Translates the ID from the Lookup ID to the Cosmos ID.
        /// </summary>
        /// <returns></returns>
        protected virtual string TranslateToId() => LookupId;
    }
}