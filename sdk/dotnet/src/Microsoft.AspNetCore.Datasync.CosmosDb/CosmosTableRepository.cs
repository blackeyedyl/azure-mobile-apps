// Copyright (c) Microsoft Corporation. All Rights Reserved.
// Licensed under the MIT License.

using Microsoft.Azure.Cosmos;
using Newtonsoft.Json.Linq;
using System;
using System.IO;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Container = Microsoft.Azure.Cosmos.Container;

namespace Microsoft.AspNetCore.Datasync.CosmosDb
{
    /// <summary>
    /// An implementation of an <see cref="IRepository{TEntity}"/> that stores
    /// data in a Cosmos database.
    /// </summary>
    /// <typeparam name="TEntity">The type of entity being stored.</typeparam>
    public class CosmosTableRepository<TEntity> : IRepository<TEntity> where TEntity : class, ITableData
    {
        /// <summary>
        /// Container where the <see cref="TEntity"/> is stored.
        /// </summary>
        private readonly Container container;

        private readonly CosmosRepositoryOptions cosmosRepositoryOptions;

        /// <summary>
        /// Create a new <see cref="CosmosTableRepository{TEntity}"/> for accessing the database.
        /// This is the normal ctor for this repository.
        /// </summary>
        /// <param name="cosmosClient">The <see cref="cosmosClient"/> for the backend store.</param>
        /// <param name="databaseName">The name of the database.</param>
        /// <param name="containerName">The name of the container.</param>
        public CosmosTableRepository(Container container, CosmosRepositoryOptions cosmosRepositoryOptions = null)
        {
            // Type check - only known derivates are allowed.
            var typeInfo = typeof(TEntity);
            if (!typeInfo.IsSubclassOf(typeof(CosmosTableData)))
            {
                throw new InvalidCastException($"Entity type {typeof(TEntity).Name} is not a valid entity type.");
            }
            if (typeof(TEntity).Name == "User")
            {
                throw new InvalidCastException("User is a reserved entity type name in Cosmos and will not work with AsQueryable() oData library.");
            }
            ArgumentNullException.ThrowIfNull(container, nameof(container));
            try
            {
                _ = container.ReadContainerAsync().ConfigureAwait(false);
            }
            catch (CosmosException cosmosException) when (cosmosException.StatusCode == System.Net.HttpStatusCode.NotFound)
            {
                throw new ArgumentException($"Container does not exist in {container.Database}: {container}", nameof(container));
            }

            this.container = container;
            this.cosmosRepositoryOptions = cosmosRepositoryOptions ?? new();
            this.cosmosRepositoryOptions.PartitionKeyPropertyNames ??= new() { "id" };
            this.cosmosRepositoryOptions.ItemRequestOptions ??= new();
            this.cosmosRepositoryOptions.ParseIdAndPartitionKey ??= CosmosUtils.DefaultParseIdAndPartitionKey;
        }

        /// <summary>
        /// Returns an unexecuted <see cref="IQueryable{T}"/> that represents the data store as a whole.
        /// This is adjusted by the <see cref="TableController{TEntity}"/> to account for filtering and
        /// paging requests.
        /// </summary>
        /// <returns>An <see cref="IQueryable{T}"/> for the entities in the data store.</returns>
        public IQueryable<TEntity> AsQueryable() => container.GetItemLinqQueryable<TEntity>(true);

        /// <summary>
        /// Returns an unexecuted <see cref="IQueryable{T}"/> that represents the data store as a whole.
        /// This is adjusted by the <see cref="TableController{TEntity}"/> to account for filtering and
        /// paging requests.
        /// </summary>
        /// <returns>An <see cref="IQueryable{T}"/> for the entities in the data store.</returns>
        public Task<IQueryable<TEntity>> AsQueryableAsync() => Task.FromResult(AsQueryable());

        /// <summary>
        /// Creates an entity within the data store. After completion, the system properties
        /// within the entity have been updated with new values.
        /// </summary>
        /// <param name="entity">The entity to be created.</param>
        /// <param name="token">A cancellation token.</param>
        /// <exception cref="ConflictException">if the entity to be created already exists.</exception>
        /// <exception cref="RepositoryException">if an error occurs in the data store.</exception>
        public async Task CreateAsync(TEntity entity, CancellationToken token = default)
        {
            ArgumentNullException.ThrowIfNull(entity, nameof(entity));

            try
            {
                var (parsedId, partitionKey) = cosmosRepositoryOptions.ParseIdAndPartitionKey(entity.Id);
                if (parsedId == null)
                {
                    entity.Id = parsedId = Guid.NewGuid().ToString("N");
                }
                entity.UpdatedAt = DateTimeOffset.UtcNow;
                var dynamicJsonEntity = await ConvertEntityToJson(entity, parsedId);
                var newEntityResponse = await container.CreateItemAsync<dynamic>(dynamicJsonEntity, requestOptions: cosmosRepositoryOptions.ItemRequestOptions, cancellationToken: token);
                var newEntity = newEntityResponse.Resource.ToObject<TEntity>();
                entity.UpdatedAt = newEntity.UpdatedAt;
                entity.Version = newEntity.Version;
            }
            catch (CosmosException cosmosException) when (cosmosException.StatusCode == HttpStatusCode.Conflict)
            {
                var storeEntity = await ReadAsync(entity.Id, token);
                throw new ConflictException(storeEntity);
            }
            catch (CosmosException cosmosException)
            {
                throw new RepositoryException(cosmosException.Message, cosmosException);
            }
        }

        /// <summary>
        /// Removes an entity from the data store. If a <c>version</c> is provided, the version
        /// must match the entity version.
        /// </summary>
        /// <param name="id">The globally unique ID of the entity to be removed.</param>
        /// <param name="version">The (optional) version of the entity to be removed.</param>
        /// <param name="token">A cancellation token.</param>
        /// <exception cref="NotFoundException">if the entity does not exist.</exception>
        /// <exception cref="PreconditionFailedException">if the entity version does not match the provided version</exception>
        /// <exception cref="RepositoryException">if an error occurs in the data store.</exception>
        public async Task DeleteAsync(string id, byte[] version = null, CancellationToken token = default)
        {
            if (string.IsNullOrEmpty(id))
            {
                throw new BadRequestException();
            }
            var (parsedId, partitionKey) = cosmosRepositoryOptions.ParseIdAndPartitionKey(id);
            if (string.IsNullOrEmpty(parsedId))
            {
                throw new BadRequestException();
            }

            try
            {
                var oneTimeRequestOptions = CreateOptionsForThisRequest(version);
                await container.DeleteItemAsync<TEntity>(parsedId, partitionKey, oneTimeRequestOptions, token);
            }
            catch (CosmosException cosmosException) when (cosmosException.StatusCode == HttpStatusCode.NotFound)
            {
                throw new NotFoundException();
            }
            catch (CosmosException cosmosException) when (cosmosException.StatusCode == HttpStatusCode.PreconditionFailed)
            {
                var storeEntity = await ReadAsync(id, token);
                throw new PreconditionFailedException(storeEntity);
            }
            catch (CosmosException cosmosException)
            {
                throw new RepositoryException(cosmosException.Message, cosmosException);
            }
        }

        /// <summary>
        /// Reads the entity from the data store.
        /// </summary>
        /// <remarks>
        /// </remarks>
        /// <param name="id">The globally unique ID of the entity to be read.</param>
        /// <param name="token">A cancellation token</param>
        /// <returns>The entity, or null if the entity does not exist.</returns>
        /// <exception cref="RepositoryException">if an error occurs in the data store.</exception>
        public async Task<TEntity> ReadAsync(string id, CancellationToken token = default)
        {
            if (string.IsNullOrEmpty(id))
            {
                throw new BadRequestException();
            }

            var (parsedId, partitionKey) = cosmosRepositoryOptions.ParseIdAndPartitionKey(id);
            if (string.IsNullOrEmpty(parsedId))
            {
                throw new BadRequestException();
            }

            try
            {
                TEntity entity = await container.ReadItemAsync<TEntity>(parsedId, partitionKey, cosmosRepositoryOptions.ItemRequestOptions, token);
                entity.Id = id;
                return entity;
            }
            catch (CosmosException cosmosException) when (cosmosException.StatusCode == HttpStatusCode.NotFound)
            {
                return null;
            }
            catch (CosmosException cosmosException)
            {
                throw new RepositoryException(cosmosException.Message, cosmosException);
            }
        }

        /// <summary>
        /// Replace the entity within the store with the provided entity.  If a <c>version</c> is
        /// specified, then the version must match.  On return, the system properties of the entity
        /// will be updated.
        /// </summary>
        /// <param name="entity">The replacement entity.</param>
        /// <param name="version">The (optional) version of the entity to be replaced</param>
        /// <param name="token">A cancellation token</param>
        /// <exception cref="BadRequestException">if the entity does not have an ID</exception>
        /// <exception cref="NotFoundException">if the entity does not exist</exception>
        /// <exception cref="ConflictException">if the entity version does not match the provided version</exception>
        /// <exception cref="RepositoryException">if an error occurs in the data store.</exception>
        public async Task ReplaceAsync(TEntity entity, byte[] version = null, CancellationToken token = default)
        {
            ArgumentNullException.ThrowIfNull(entity, nameof(entity));
            var (parsedId, partitionKey) = cosmosRepositoryOptions.ParseIdAndPartitionKey(entity.Id);
            if (string.IsNullOrEmpty(parsedId))
            {
                throw new BadRequestException();
            }

            try
            {
                entity.UpdatedAt = DateTimeOffset.UtcNow;

                var dynamicJsonEntity = await ConvertEntityToJson(entity, parsedId);

                var oneTimeRequestOptions = CreateOptionsForThisRequest(version);
                var newObject = await container.ReplaceItemAsync<dynamic>(dynamicJsonEntity, parsedId, requestOptions: oneTimeRequestOptions, cancellationToken: token);
                var newEntity = newObject.Resource.ToObject<TEntity>();

                entity.UpdatedAt = newEntity.UpdatedAt;
                entity.Version = newEntity.Version;
            }
            catch (CosmosException cosmosException) when (cosmosException.StatusCode == HttpStatusCode.NotFound)
            {
                throw new NotFoundException();
            }
            catch (CosmosException cosmosException) when (cosmosException.StatusCode == HttpStatusCode.PreconditionFailed)
            {
                var storeEntity = await ReadAsync(entity.Id, token);
                throw new PreconditionFailedException(storeEntity);
            }
            catch (CosmosException cosmosException)
            {
                throw new RepositoryException(cosmosException.Message, cosmosException);
            }
        }

        /// <summary>
        /// Converts the <paramref name="entity"/> to a json object and sets the id
        /// to the <paramref name="lookupId"/> using the Comsos DB serializer.
        /// </summary>
        /// <param name="entity">Entity to convert.</param>
        /// <param name="lookupId"></param>
        /// <returns></returns>
        internal async Task<JObject> ConvertEntityToJson(TEntity entity, string lookupId)
        {
            ArgumentNullException.ThrowIfNull(entity, nameof(entity));
            if (string.IsNullOrEmpty(lookupId))
            {
                throw new ArgumentException("Cannot be null or empty", nameof(lookupId));
            }
            using var stream = container.Database.Client.ClientOptions.Serializer.ToStream(entity);
            var reader = new StreamReader(stream, Encoding.UTF8);
            var jsonString = await reader.ReadToEndAsync();
            var jObjectEntity = JObject.Parse(jsonString);
            
            // Set IDs to lookupId so that we don't save a constructed ID
            jObjectEntity["id"] = lookupId;

            return jObjectEntity;
        }

        /// <summary>
        /// Copies the <see cref="ItemRequestOptions"/> from <see cref="cosmosRepositoryOptions"/> and sets the IfMatchEtag 
        /// to the provided version if not null. This is used to verify the precondition for the request.
        /// </summary>
        /// <param name="version"></param>
        /// <returns></returns>
        internal ItemRequestOptions CreateOptionsForThisRequest(byte[] version)
        {
            var oneTimeOptions = cosmosRepositoryOptions.ItemRequestOptions.ShallowCopy() as ItemRequestOptions;
            if (version != null)
            {
                oneTimeOptions.IfMatchEtag = Encoding.UTF8.GetString(version);
            }
            return oneTimeOptions;
        }
    }
}