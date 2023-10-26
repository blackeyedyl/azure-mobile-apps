// Copyright (c) Microsoft Corporation. All Rights Reserved.
// Licensed under the MIT License.

using Microsoft.AspNetCore.Datasync.CosmosDb.Test.Helpers;

namespace Microsoft.AspNetCore.Datasync.CosmosDb.Test;

[ExcludeFromCodeCoverage]
public class CosmosTableRepository_Tests : IDisposable
{
    private readonly CosmosMovieWithPartitionKey blackPantherMovie = new()
    {
        BestPictureWinner = true,
        Duration = 134,
        Rating = "PG-13",
        ReleaseDate = DateTimeOffset.Parse("16-Feb-2018"),
        Title = "Black Panther",
        Year = 2018
    };

    internal class NotEntityModel : ITableData
    {
        public string Id { get => throw new NotImplementedException(); set => throw new NotImplementedException(); }
        public byte[] Version { get => throw new NotImplementedException(); set => throw new NotImplementedException(); }
        public DateTimeOffset UpdatedAt { get => throw new NotImplementedException(); set => throw new NotImplementedException(); }
        public bool Deleted { get => throw new NotImplementedException(); set => throw new NotImplementedException(); }

        public bool Equals(ITableData other)
        {
            throw new NotImplementedException();
        }
    }
    private Container movieContainer;
    private CosmosTableRepository<CosmosMovieWithPartitionKey> repository;
    private List<string> partitionKeyPropertyNames = new() { "Rating" };

    public CosmosTableRepository_Tests()
    {
        movieContainer = CosmosDbHelper.GetContainer().Result;
        CosmosRepositoryOptions cosmosRepositoryOptions = new()
        {
            PartitionKeyPropertyNames = partitionKeyPropertyNames
        };
        repository = new(movieContainer, cosmosRepositoryOptions);
    }

    [SuppressMessage("Usage", "CA1816:Dispose methods should call SuppressFinalize", Justification = "Test Case - no inherited classes")]
    public void Dispose()
    {
        movieContainer.Database.DeleteAsync().Wait();
    }

    [Fact]
    public void CosmosTableRepository_CanCreate_WithContainer()
    {
        // Assert
        Assert.NotNull(repository);
    }

    [Fact]
    public void CosmosTableRepository_Throws_WithNullContainer()
    {
        // Assert
        Assert.Throws<ArgumentNullException>(() => new CosmosTableRepository<CosmosMovieWithPartitionKey>(null));
    }

    [Fact]
    public void CosmosTableRepository_Throws_OnNonSupported_GenericParam()
    {
        Assert.Throws<InvalidCastException>(() => new CosmosTableRepository<NotEntityModel>(movieContainer));
    }

    [Fact]
    public void AsQueryable_Returns_IQueryable()
    {
        // Act
        var actual = repository.AsQueryable();
        
        // Assert
        Assert.IsAssignableFrom<IQueryable<CosmosMovieWithPartitionKey>>(actual);
        Assert.Equal(Movies.Count, actual.Count());
        Assert.Contains(":", actual.ToList().First().Id);
    }

    [Fact]
    public async void AsQueryableAsync_Returns_IQueryable()
    {
        // Act
        var actual = await repository.AsQueryableAsync();
       
        // Assert
        Assert.IsAssignableFrom<IQueryable<CosmosMovieWithPartitionKey>>(actual);
        Assert.Equal(Movies.Count, actual.Count());
        Assert.Contains(":", actual.ToList().First().Id);
    }

    [Fact]
    public void AsQueryable_CanRetrieveFilteredLists()
    {
        // Act
        var ratedMovies = repository.AsQueryable().Where(m => m.Rating == "R").ToList();

        // Assert
        Assert.Equal(95, ratedMovies.Count);
        Assert.EndsWith(":R", ratedMovies.First().Id);
    }

    [Fact]
    public async Task CreateAsync_CreatesNewEntity_WithNullId()
    {
        // Arrange
        var item = blackPantherMovie.Clone();

        // Act
        await repository.CreateAsync(item);

        // Assert
        Assert.Equal<IMovie>(blackPantherMovie, item);
        Assert.EndsWith($":{item.Rating}", item.Id);
        Assert.True(Guid.TryParse(item.Id.Split(':')[0], out _));
        AssertEx.SystemPropertiesSet(item);

    }

    [Fact]
    public async Task CreateAsync_ThrowsConflict()
    {
        // Arrange
        var movie = Movies.GetRandomMovie<CosmosMovieWithPartitionKey>();
        var item = blackPantherMovie.Clone();
        item.Id = movie.Id;
        item.Rating = movie.Rating;
        
        // Act & Assert
        var ex = await Assert.ThrowsAsync<ConflictException>(() => repository.CreateAsync(item));
        
        var (id, partitionKey) = CosmosUtils.DefaultParseIdAndPartitionKey(movie.Id);
        CosmosMovieWithPartitionKey entity = await movieContainer.ReadItemAsync<CosmosMovieWithPartitionKey>(id, partitionKey);
        Assert.NotSame(entity, ex.Payload);
        Assert.Equal(entity, ex.Payload as IMovie);
        Assert.Equal(entity, ex.Payload as ITableData);
    }

    [Fact]
    public async Task CreateAsync_UpdatesUpdatedAt()
    {
        // Arrange
        var item = blackPantherMovie.Clone();
        item.UpdatedAt = DateTimeOffset.UtcNow.AddMonths(-1);

        // Act
        await repository.CreateAsync(item);

        // Assert
        Assert.Equal<IMovie>(blackPantherMovie, item);
        AssertEx.SystemPropertiesSet(item);
    }

    [Fact]
    public async Task CreateAsync_UpdatesVersion()
    {
        // Arrange
        var item = blackPantherMovie.Clone();
        var version = Guid.NewGuid().ToByteArray();
        item.Version = version.ToArray();

        // Act
        await repository.CreateAsync(item);

        // Assert
        Assert.Equal<IMovie>(blackPantherMovie, item);
        AssertEx.SystemPropertiesSet(item);
        Assert.False(item.Version.SequenceEqual(version));
    }

    [Fact]
    public async Task DeleteAsync_Deletes_WhenNoVersion()
    {
        // Arrange
        var item = Movies.GetRandomMovie<CosmosMovieWithPartitionKey>();

        // Act
        await repository.DeleteAsync(item.Id);

        // Assert
        var ex = await Assert.ThrowsAsync<CosmosException>(() => movieContainer.ReadItemAsync<CosmosMovieWithPartitionKey>(item.Id, new(item.Id)));
        Assert.Equal(System.Net.HttpStatusCode.NotFound, ex.StatusCode);
    }

    [Fact]
    public async Task DeleteAsync_Throws_WhenEntityVersionsDiffer()
    {
        // Arrange
        var item = Movies.GetRandomMovie<CosmosMovieWithPartitionKey>();
        var version = Guid.NewGuid().ToByteArray();

        // Act & Assert
        var ex = await Assert.ThrowsAsync<PreconditionFailedException>(() => repository.DeleteAsync(item.Id, version));

        // Assert
        var (id, partitionKey) = CosmosUtils.DefaultParseIdAndPartitionKey(item.Id);
        var entity = await movieContainer.ReadItemAsync<CosmosMovieWithPartitionKey>(id, partitionKey);
        Assert.NotNull(entity);
        Assert.NotNull(ex.Payload);
        Assert.NotSame(entity, ex.Payload);
    }

    [Fact]
    public async Task ReadAsync_Throws_OnNullId()
    {
        // Act & Assert
        _ = await Assert.ThrowsAsync<BadRequestException>(() => repository.ReadAsync(null));
    }

    [Fact]
    public async Task ReadAsync_Throws_OnEmptyId()
    {
        // Act & Assert
        _ = await Assert.ThrowsAsync<BadRequestException>(() => repository.ReadAsync(""));
    }

    [Theory]
    [InlineData("id")]
    [InlineData("id-0000")]
    [InlineData("id-000 is super long")]
    [InlineData("id-300")]
    [InlineData("id-300:PG-13")]
    [InlineData("id-123:R")]
    public async Task ReadAsync_ReturnsNull_IfMissing(string id)
    {
        // Act
        var actual = await repository.ReadAsync(id);

        Assert.Null(actual);
    }

    [Fact]
    public async Task ReplaceAsync_Throws_OnNullEntity()
    {
        // Act & Assert
        await Assert.ThrowsAsync<ArgumentNullException>(() => repository.ReplaceAsync(null));
    }

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    public async Task ReplaceAsync_Throws_OnNullId(string id)
    {
        // Arrange
        var entity = blackPantherMovie.Clone();
        entity.Id = id;

        // Act & Assert
        await Assert.ThrowsAsync<BadRequestException>(() => repository.ReplaceAsync(entity));
    }

    [Theory]
    [InlineData("id")]
    [InlineData("id-0000")]
    [InlineData("id-000 is super long")]
    [InlineData("id-300")]
    [InlineData("id-300:PG-13")]
    [InlineData("id-123:R")]
    public async Task ReplaceAsync_Throws_OnMissingEntity(string id)
    {
        // Arrange
        var entity = blackPantherMovie.Clone();
        entity.Id = id;

        // Act & Assert
        await Assert.ThrowsAsync<NotFoundException>(() => repository.ReplaceAsync(entity));
    }

    [Fact]
    public async Task ReplaceAsync_Throws_OnVersionMismatch()
    {
        // Arrange
        var entity = blackPantherMovie.Clone();
        var movie = Movies.GetRandomMovie<CosmosMovieWithPartitionKey>();
        entity.Id = movie.Id;
        entity.Rating = movie.Rating;
        var version = Guid.NewGuid().ToByteArray();

        // Act & Assert
        var ex = await Assert.ThrowsAsync<PreconditionFailedException>(() => repository.ReplaceAsync(entity, version));

        // Assert
        var (id, partitionKey) = CosmosUtils.DefaultParseIdAndPartitionKey(movie.Id);
        CosmosMovieWithPartitionKey expected = await movieContainer.ReadItemAsync<CosmosMovieWithPartitionKey>(id, partitionKey);
        Assert.NotSame(expected, ex.Payload);
        Assert.Equal(expected, ex.Payload as IMovie);
        Assert.Equal(expected, ex.Payload as ITableData);
    }

    [Fact]
    public async Task ReplaceAsync_Replaces_OnVersionMatch()
    {
        // Arrange
        var movie = Movies.GetRandomMovie<CosmosMovieWithPartitionKey>();
        var (id, partitionKey) = CosmosUtils.DefaultParseIdAndPartitionKey(movie.Id);
        CosmosMovieWithPartitionKey original = await movieContainer.ReadItemAsync<CosmosMovieWithPartitionKey>(id, partitionKey);

        var entity = blackPantherMovie.Clone();
        entity.Id = movie.Id;
        entity.Rating = movie.Rating;
        var version = original.Version.ToArray();

        // Act
        await repository.ReplaceAsync(entity, version);

        // Assert
        var (expectedId, expectedKey) = CosmosUtils.DefaultParseIdAndPartitionKey(entity.Id);
        CosmosMovieWithPartitionKey expected = await movieContainer.ReadItemAsync<CosmosMovieWithPartitionKey>(expectedId, expectedKey);
        Assert.NotSame(expected, entity);
        Assert.Equal<IMovie>(expected, entity);
        AssertEx.SystemPropertiesChanged(original, entity);
    }

    [Fact]
    public async Task ReplaceAsync_Replaces_OnNoVersion()
    {
        // Arrange
        var entity = blackPantherMovie.Clone();
        var movie = Movies.GetRandomMovie<CosmosMovieWithPartitionKey>();
        entity.Id = movie.Id;
        entity.Rating = movie.Rating;
        var (id, partitionKey) = CosmosUtils.DefaultParseIdAndPartitionKey(movie.Id);
        CosmosMovieWithPartitionKey originalFromDatabase = await movieContainer.ReadItemAsync<CosmosMovieWithPartitionKey>(id, partitionKey);
        var original = originalFromDatabase.Clone();

        // Act
        await repository.ReplaceAsync(entity);

        // Assert
        var (expectedId, expectedKey) = CosmosUtils.DefaultParseIdAndPartitionKey(entity.Id);
        CosmosMovieWithPartitionKey expected = await movieContainer.ReadItemAsync<CosmosMovieWithPartitionKey>(expectedId, expectedKey);
        Assert.NotSame(expected, entity);
        Assert.Equal<IMovie>(expected, entity);
        AssertEx.SystemPropertiesChanged(original, entity);
    }

    [Fact]
    public async Task ConvertEntityToJson_ShouldReturnCorrectIsoDate()
    {
        // Arrange
        var entity = Movies.GetRandomMovie<CosmosMovieWithPartitionKey>();
        var lookupId = entity.Id.Split(':')[0];
        entity.UpdatedAt = DateTimeOffset.UtcNow;

        // Act
        var result = await repository.ConvertEntityToJson(entity, lookupId);

        // Assert
        Assert.NotNull(result);

        string expectedDateFormat = entity.UpdatedAt.ToString("yyyy-MM-ddTHH:mm:ss.fffffffZ");
        Assert.Equal(expectedDateFormat, result["UpdatedAt"].ToString());
    }

    [Fact]
    public async Task ConvertEntityToJson_ShouldReturnCorrectJObject()
    {
        // Arrange
        var entity = Movies.GetRandomMovie<CosmosMovieWithPartitionKey>();
        var lookupId = entity.Id.Split(':')[0];

        // Act
        var result = await repository.ConvertEntityToJson(entity, lookupId);

        // Assert
        Assert.NotNull(result);
        Assert.Equal(lookupId, result["id"].ToString());
    }

    [Fact]
    public async Task ConvertEntityToJson_ShouldThrowArgumentNullException_WhenEntityIsNull()
    {
        // Arrange
        var lookupId = "id-123";

        // Act & Assert
        await Assert.ThrowsAsync<ArgumentNullException>(() => repository.ConvertEntityToJson(null, lookupId));
    }

    [Fact]
    public async Task ConvertEntityToJson_ShouldThrowArgumentNullException_WhenIdIsNullOrEmpty()
    {
        // Arrange
        var entity = Movies.GetRandomMovie<CosmosMovieWithPartitionKey>();

        // Act & Assert
        await Assert.ThrowsAsync<ArgumentException>(() => repository.ConvertEntityToJson(entity, null));
        await Assert.ThrowsAsync<ArgumentException>(() => repository.ConvertEntityToJson(entity, string.Empty));
    }
}
